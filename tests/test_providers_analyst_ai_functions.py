from __future__ import annotations

import pathlib
import sys

import pytest

REPO_SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

from lfx_snowflake.components import (  # noqa: E402
    AI_REDACT_LIMITATIONS_DISCLAIMER,
    BatchRunComponent,
    BoundedSqlResultPolicy,
    CortexAnalystService,
    DataSourceObjectStatus,
    GuardrailsComponent,
    LLM_SELECTOR_INVARIANT_SPEC,
    SmartRouterComponent,
    SmartTransformComponent,
    SnowflakeAIFunctionComponent,
    SnowflakeDataSourcesComponent,
    SnowflakePostgresDataSourceAdapter,
    StructuredOutputComponent,
)
from lfx_snowflake.providers import (  # noqa: E402
    ERROR_AUTHZ_ROLE_DENIED,
    ERROR_OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
    ERROR_TECHNICAL_EXECUTION_ERROR,
    ERROR_UNSUPPORTED_CAPABILITY,
    EmbeddingSpaceGuard,
    SnowflakeEmbeddingModelAdapter,
    SnowflakeOllamaAdapter,
    SnowflakeProviderError,
    SnowflakeProviderRegistry,
    SnowflakeProxyChatAdapter,
    SnowflakeProxyMessagesAdapter,
    SnowflakeProxyResponsesAdapter,
    deduplicate_tool_call_ids,
)


def test_registry_discovery_and_role_switch_invalidation() -> None:
    registry = SnowflakeProviderRegistry(
        proxy_base_url="http://llm-proxy-standard.local:8080",
        ollama_base_url="http://ollama-service.local:11434",
        active_role="ROLE_ANALYST",
        allowed_roles={"ROLE_ANALYST"},
    )
    providers = registry.list_providers()
    assert len(providers) == 4
    assert {p.display_name for p in providers} == {
        "Snowflake Proxy - Responses",
        "Snowflake Proxy - Messages",
        "Snowflake Proxy - Chat",
        "Ollama (Snowflake Hosted)",
    }

    chat_models = [m.id for m in registry.discover_models("chat")]
    assert "grok-4.7" in chat_models
    assert "openai-gpt-6-luna" in chat_models

    registry.replay_cache.put("rs_1", {"id": "rs_1", "encrypted_content": "cipher"})
    assert registry.replay_cache.get("rs_1") is not None

    initial_epoch = registry.context_epoch
    registry.allowed_roles = {"ROLE_ANALYST", "ROLE_DATA_ENG"}
    new_epoch = registry.switch_role("ROLE_DATA_ENG")
    assert new_epoch == initial_epoch + 1
    assert registry.active_role == "ROLE_DATA_ENG"
    assert registry.replay_cache.get("rs_1") is None


def test_registry_role_denied_returns_explicit_403_message() -> None:
    registry = SnowflakeProviderRegistry(
        active_role="ROLE_RESTRICTED",
        allowed_roles={"ROLE_AUTHORIZED"},
    )
    with pytest.raises(SnowflakeProviderError) as exc_info:
        registry.list_providers()
    err = exc_info.value
    assert err.code == ERROR_AUTHZ_ROLE_DENIED
    assert err.status_code == 403
    assert "Il ruolo attivo (`ROLE_RESTRICTED`) non consente questa operazione" in err.message


def test_multiround_tool_call_id_deduplication() -> None:
    history = [
        {
            "role": "assistant",
            "tool_calls": [
                {"id": "call_0", "type": "function", "function": {"name": "q1", "arguments": "{}"}},
                {"id": "call_1", "type": "function", "function": {"name": "q2", "arguments": "{}"}},
            ],
        },
        {"role": "tool", "tool_call_id": "call_0", "content": "r1"},
        {"role": "tool", "tool_call_id": "call_1", "content": "r2"},
        {
            "role": "assistant",
            "tool_calls": [
                {"id": "call_0", "type": "function", "function": {"name": "q3", "arguments": "{}"}},
                {"id": "call_1", "type": "function", "function": {"name": "q4", "arguments": "{}"}},
            ],
        },
        {"role": "tool", "tool_call_id": "call_0", "content": "r3"},
        {"role": "tool", "tool_call_id": "call_1", "content": "r4"},
    ]
    deduped = deduplicate_tool_call_ids(history)
    # Round 1 keeps call_0, call_1
    assert deduped[0]["tool_calls"][0]["id"] == "call_0"
    assert deduped[1]["tool_call_id"] == "call_0"
    # Round 2 renames deterministically to call_0_r2, call_1_r2 and updates matching tool outputs
    assert deduped[3]["tool_calls"][0]["id"] == "call_0_r2"
    assert deduped[3]["tool_calls"][1]["id"] == "call_1_r2"
    assert deduped[4]["tool_call_id"] == "call_0_r2"
    assert deduped[5]["tool_call_id"] == "call_1_r2"


def test_proxy_chat_adapter_normalization_and_zero_retry() -> None:
    registry = SnowflakeProviderRegistry(
        proxy_base_url="http://llm-proxy-standard.local:8080",
        active_role="ROLE_ANALYST",
    )
    chat = SnowflakeProxyChatAdapter(registry)
    assert chat.base_url == "http://llm-proxy-standard.local:8080/chat/v1"
    assert chat.discovery_url == "http://llm-proxy-standard.local:8080/chat/v1/models"
    assert chat.inference_url == "http://llm-proxy-standard.local:8080/chat/v1/chat/completions"

    # Luna requires reasoning_effort="none" even if caller passes "medium", and resolves max_tokens
    luna_payload = chat.build_request_payload(
        "openai-gpt-6-luna",
        [{"role": "user", "content": "hello"}],
        stream=True,
        max_tokens=2048,
        reasoning_effort="medium",
    )
    assert luna_payload["reasoning_effort"] == "none"
    assert luna_payload["max_completion_tokens"] == 2048
    assert "max_tokens" not in luna_payload
    assert luna_payload["stream_options"] == {"include_usage": True}

    # Grok-4.7 strips reasoning_effort
    grok_payload = chat.build_request_payload(
        "grok-4.7",
        [{"role": "user", "content": "hello"}],
        reasoning_effort="high",
    )
    assert "reasoning_effort" not in grok_payload

    # Zero retry on transport failure
    call_counter = {"count": 0}

    def failing_transport(url: str, payload: dict, headers: dict) -> dict:
        call_counter["count"] += 1
        raise RuntimeError("HTTP 429 rate limit")

    chat_fail = SnowflakeProxyChatAdapter(registry, transport=failing_transport)
    with pytest.raises(SnowflakeProviderError) as exc_info:
        chat_fail.invoke("grok-4.7", [{"role": "user", "content": "ping"}])
    assert exc_info.value.code == ERROR_TECHNICAL_EXECUTION_ERROR
    assert call_counter["count"] == 1


def test_proxy_responses_adapter_ordered_items_and_encrypted_content_replay() -> None:
    registry = SnowflakeProviderRegistry(
        proxy_base_url="http://llm-proxy-standard.local:8080",
        active_role="ROLE_ANALYST",
    )
    responses = SnowflakeProxyResponsesAdapter(registry)
    assert responses.base_url == "http://llm-proxy-standard.local:8080/responses/v1"
    assert responses.use_responses_api is True

    # Caller sends store=True, temperature=0.7, empty text={} -> normalized out
    payload = responses.build_request_payload(
        "openai-gpt-6-astra",
        [{"role": "user", "content": "run tool"}],
        store=True,
        temperature=0.7,
        text={},
    )
    assert payload["store"] is False
    assert payload["response_stream"] == "ordered-items"
    assert "reasoning.encrypted_content" in payload["include"]
    assert "temperature" not in payload
    assert "text" not in payload

    # Record output items in arbitrary order -> ordered-items puts reasoning before function_call
    recorded = responses.record_response_items(
        [
            {"id": "fc_1", "type": "function_call", "call_id": "call_0", "name": "lookup", "arguments": "{}"},
            {"id": "rs_1", "type": "reasoning", "encrypted_content": "enc_blob_xyz"},
        ]
    )
    assert recorded[0]["type"] == "reasoning"
    assert recorded[1]["type"] == "function_call"

    # Next turn: stateless caller omits reasoning block -> adapter re-injects encrypted_content
    turn2_payload = responses.build_request_payload(
        "openai-gpt-6-astra",
        [
            {"type": "function_call", "call_id": "call_0", "name": "lookup", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "call_0", "output": "ok"},
        ],
    )
    assert turn2_payload["input"][0]["type"] == "reasoning"
    assert turn2_payload["input"][0]["encrypted_content"] == "enc_blob_xyz"

    # Editing/deleting signed message in Settings -> Messages invalidates replay cache & signals branch
    invalidation = registry.replay_cache.invalidate_on_message_edit_or_delete("call_0")
    assert invalidation["invalidated"] is True
    assert invalidation["requires_branch"] is True
    assert registry.replay_cache.get("call_0") is None


def test_proxy_messages_adapter_urls_thinking_signature_and_tool_choice() -> None:
    registry = SnowflakeProviderRegistry(
        proxy_base_url="http://llm-proxy-standard.local:8080",
        active_role="ROLE_ANALYST",
    )
    messages_adapter = SnowflakeProxyMessagesAdapter(registry)
    assert messages_adapter.sdk_base_url == "http://llm-proxy-standard.local:8080/messages"
    assert messages_adapter.inference_url == "http://llm-proxy-standard.local:8080/messages/v1/messages"
    assert messages_adapter.discovery_url == "http://llm-proxy-standard.local:8080/messages/v1/models"

    # Record assistant response with thinking.signature + tool_use
    messages_adapter.record_response_blocks(
        [
            {"type": "thinking", "thinking": "step 1", "signature": "sig_claude_123"},
            {"type": "redacted_thinking", "data": "redacted_456"},
            {"type": "tool_use", "id": "toolu_01", "name": "get_weather", "input": {}},
        ]
    )

    # Next turn: stateless caller stripped thinking blocks -> adapter rehydrates them
    payload = messages_adapter.build_request_payload(
        "claude-sonnet-5-5",
        [
            {"role": "user", "content": "weather?"},
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "toolu_01", "name": "get_weather", "input": {}}],
            },
            {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "toolu_01", "content": "sunny"}],
            },
        ],
        thinking={"type": "enabled", "budget_tokens": 2048},
        max_tokens=1024,
        temperature=0.5,
        tools=[{"name": "get_weather", "input_schema": {"type": "object"}}],
        tool_choice="auto",
    )
    # max_tokens automatically raised above thinking.budget_tokens, temperature stripped, tool_choice objectified
    assert payload["max_tokens"] > 2048
    assert "temperature" not in payload
    assert payload["tool_choice"] == {"type": "auto"}
    assistant_blocks = payload["messages"][1]["content"]
    assert assistant_blocks[0]["type"] == "thinking"
    assert assistant_blocks[0]["signature"] == "sig_claude_123"
    assert assistant_blocks[1]["type"] == "redacted_thinking"

    # Forced tool_choice ("any") is rejected
    with pytest.raises(SnowflakeProviderError) as exc_info:
        messages_adapter.build_request_payload(
            "claude-opus-5-5",
            [{"role": "user", "content": "hi"}],
            tools=[{"name": "t1", "input_schema": {"type": "object"}}],
            tool_choice={"type": "any"},
        )
    assert exc_info.value.code == ERROR_UNSUPPORTED_CAPABILITY


def test_ollama_and_embedding_space_guard() -> None:
    registry = SnowflakeProviderRegistry(active_role="ROLE_ANALYST")
    ollama = SnowflakeOllamaAdapter(registry)
    assert "llama3.3:70b" in ollama.list_generative_models()
    assert "nomic-embed-text:latest" not in ollama.list_generative_models()

    # Using embedding-only model as generative fails explicitly
    with pytest.raises(SnowflakeProviderError) as exc_info:
        ollama.build_chat_payload("nomic-embed-text:latest", [{"role": "user", "content": "hi"}])
    assert exc_info.value.code == ERROR_UNSUPPORTED_CAPABILITY

    embedder = SnowflakeEmbeddingModelAdapter(registry)
    models = embedder.list_embedding_models()
    model_ids = {m.model_id for m in models}
    assert "snowflake-arctic-embed-l-v2.0" in model_ids
    assert "snowflake-arctic-embed-m-v1.5" in model_ids
    assert "e5-base-v2" in model_ids
    assert "nomic-embed-text:latest" in model_ids

    # Generative model rejected as embedder
    with pytest.raises(SnowflakeProviderError):
        embedder.get_embedding_spec("openai-gpt-6-luna")

    # Embedding space guard blocks same-dimension (768d) different-model vector reuse
    spec_arctic_768 = embedder.get_embedding_spec("snowflake-arctic-embed-m-v1.5")
    spec_e5_768 = embedder.get_embedding_spec("e5-base-v2")
    assert spec_arctic_768.dimension == 768
    assert spec_e5_768.dimension == 768
    with pytest.raises(SnowflakeProviderError) as mismatch_exc:
        EmbeddingSpaceGuard.assert_compatible(spec_arctic_768, spec_e5_768)
    assert "Semantic vector space mismatch" in mismatch_exc.value.message

    res = embedder.embed_documents(
        "snowflake-arctic-embed-l-v2.0",
        ["Snowflake governed Langflow"],
        target_index_metadata=embedder.get_embedding_spec("snowflake-arctic-embed-l-v2.0"),
    )
    assert len(res["vectors"]) == 1
    assert len(res["vectors"][0]) == 1024


def test_data_sources_and_cortex_analyst_boundaries_and_multiturn() -> None:
    catalog = [
        DataSourceObjectStatus(
            database="APP_DB",
            schema="ANALYTICS",
            name="REVENUE_FACT",
            object_type="STANDARD_TABLE",
            columns=("ORDER_ID", "AMOUNT"),
            allowed_roles_visible=("ROLE_ANALYST", "ROLE_VIEWER"),
            allowed_roles_select=("ROLE_ANALYST",),
            semantic_view_fqn=None,  # Visible + SELECT-able, but NO Semantic View
        ),
        DataSourceObjectStatus(
            database="APP_DB",
            schema="ANALYTICS",
            name="REVENUE_SEMANTIC_READY",
            object_type="DYNAMIC_TABLE",
            columns=("ORDER_ID", "AMOUNT"),
            allowed_roles_visible=("ROLE_ANALYST",),
            allowed_roles_select=("ROLE_ANALYST",),
            semantic_view_fqn="APP_DB.ANALYTICS.SV_REVENUE",
        ),
    ]
    pg_adapter = SnowflakePostgresDataSourceAdapter(
        instance_host="pg-instance.local",
        active_role="ROLE_ANALYST",
        pg_role_mappings={"ROLE_ANALYST": "pg_analyst_user"},
    )
    ds = SnowflakeDataSourcesComponent(
        active_role="ROLE_ANALYST",
        catalog=catalog,
        postgres_adapter=pg_adapter,
    )

    # 1. Selecting a table without a Semantic View is NOT ready for Cortex Analyst
    raw_status = ds.inspect_object_readiness("APP_DB.ANALYTICS.REVENUE_FACT")
    assert raw_status["catalog_visible"] is True
    assert raw_status["select_permitted"] is True
    assert raw_status["analyst_semantic_ready"] is False

    # 2. Automatic creation of Semantic Views or invented relationships is strictly forbidden [G-A5]
    with pytest.raises(SnowflakeProviderError) as auto_sv_exc:
        ds.inspect_object_readiness("APP_DB.ANALYTICS.REVENUE_FACT", auto_create_semantic_view=True)
    assert auto_sv_exc.value.code == ERROR_UNSUPPORTED_CAPABILITY

    # 3. Postgres adapter forbids Cortex Analyst and cross-engine joins [G-A5]
    with pytest.raises(SnowflakeProviderError):
        pg_adapter.validate_query("SELECT 1", allow_analyst=True)
    with pytest.raises(SnowflakeProviderError):
        pg_adapter.validate_query("SELECT 1", join_snowflake_objects=["APP_DB.ANALYTICS.REVENUE_FACT"])

    analyst = CortexAnalystService(
        ds,
        policy=BoundedSqlResultPolicy(max_rows_execution=10, max_rows_for_synthesis=2, max_chars_for_synthesis=200),
    )

    # Binding table without Semantic View to Analyst fails
    with pytest.raises(SnowflakeProviderError):
        analyst.bind_data_source("APP_DB.ANALYTICS.REVENUE_FACT")

    # Binding semantic-ready object succeeds
    analyst.bind_data_source("APP_DB.ANALYTICS.REVENUE_SEMANTIC_READY")
    gen = analyst.generate_sql("What is total revenue?")
    assert gen["executed"] is False
    assert gen["request_payload"]["semantic_view"] == "APP_DB.ANALYTICS.SV_REVENUE"

    # Read-only enforcement on SQL execution
    with pytest.raises(SnowflakeProviderError):
        analyst.execute_generated_sql("DROP TABLE APP_DB.ANALYTICS.REVENUE_SEMANTIC_READY")

    # Bounded SQL results for multi-turn synthesis
    many_rows = [{"row": i, "amount": i * 100} for i in range(5)]
    analyst._sql_executor = lambda sql, role, meta: many_rows
    exec_res = analyst.execute_generated_sql(gen["generated_sql"])
    assert exec_res["bounded_summary"]["total_rows_returned"] == 5
    assert exec_res["bounded_summary"]["synthesis_rows_included"] == 2
    assert exec_res["bounded_summary"]["truncated_for_synthesis"] is True

    synth_ctx = analyst.build_synthesis_context_for_protocol("responses")
    assert synth_ctx["analyst_history_turns"] == 2
    assert len(synth_ctx["bounded_sql_results"]) == 1

    # Role switch invalidates Analyst session histories and bounded summaries
    ds.switch_role("ROLE_VIEWER")
    synth_after_switch = analyst.build_synthesis_context_for_protocol("responses")
    assert synth_after_switch["analyst_history_turns"] == 0
    assert len(synth_after_switch["bounded_sql_results"]) == 0


def test_llm_operations_suite_and_invariant_selector() -> None:
    assert LLM_SELECTOR_INVARIANT_SPEC["modified"] is False
    registry = SnowflakeProviderRegistry(active_role="ROLE_ANALYST")

    # Batch Run partial error without retries
    batch = BatchRunComponent(registry, max_concurrency=4, max_batch_rows=5)
    table = [{"id": 1, "text": "ok row"}, {"id": 2, "text": "fail row"}]

    def eval_row(idx: int, instr: str, val: str) -> str:
        if "fail" in val:
            raise ValueError("row failure")
        return f"processed:{val}"

    batch_out = batch.run_batch(
        provider="Snowflake Proxy - Chat",
        model="grok-4.7",
        instructions="summarize",
        table=table,
        column_name="text",
        row_evaluator=eval_row,
    )
    rows = batch_out["LLM Results"]
    assert rows[0]["status"] == "ok"
    assert rows[1]["status"] == "error"
    assert rows[1]["retries_attempted"] == 0

    # Guardrails fail-closed on runtime exception
    guardrails = GuardrailsComponent(registry)
    gr_out = guardrails.evaluate(
        provider="Snowflake Proxy - Messages",
        model="claude-sonnet-5-5",
        checking_method="llm",
        guardrails_rules=["no_pii"],
        input_text="sensitive text",
        evaluator_fn=lambda txt, rules: (_ for _ in ()).throw(RuntimeError("upstream timeout")),
    )
    assert gr_out["Pass"] is None
    assert gr_out["Fail"] == "sensitive text"
    assert gr_out["Result Data"]["status"] == "error_fail_closed"

    # Smart Router rejects unknown route
    router = SmartRouterComponent(registry)
    with pytest.raises(SnowflakeProviderError):
        router.route(
            provider="Snowflake Proxy - Responses",
            model="openai-gpt-6-astra",
            input_text="hello",
            routes=["billing", "support"],
            model_selected_route="unconfigured_route",
        )
    valid_route = router.route(
        provider="Snowflake Proxy - Responses",
        model="openai-gpt-6-astra",
        input_text="invoice question",
        routes=["billing", "support"],
        model_selected_route="billing",
    )
    assert valid_route["outputs"]["billing"] == "invoice question"
    assert valid_route["outputs"]["support"] is None

    # Smart Transform blocks imports and dunders
    transform = SmartTransformComponent(registry)
    with pytest.raises(SnowflakeProviderError):
        transform.transform(
            provider="Ollama (Snowflake Hosted)",
            model="llama3.3:70b",
            json_input={"a": 1},
            instructions="hack",
            generated_code="import os\ndef transform(data):\n    return os.environ",
        )
    clean_tx = transform.transform(
        provider="Ollama (Snowflake Hosted)",
        model="llama3.3:70b",
        json_input={"items": [1, 2, 3]},
        instructions="sum items",
        generated_code="def transform(data):\n    return {'total': sum(data['items'])}",
    )
    assert clean_tx["Output"] == {"total": 6}

    # Structured Output strict validation without silent repair
    structured = StructuredOutputComponent(registry)
    schema = {
        "type": "object",
        "required": ["ticket_id", "priority"],
        "properties": {"ticket_id": {"type": "string"}, "priority": {"type": "integer"}},
        "additionalProperties": False,
    }
    with pytest.raises(SnowflakeProviderError):
        structured.generate_structured(
            provider="Snowflake Proxy - Chat",
            model="openai-gpt-6-luna",
            input_message="create ticket",
            output_schema=schema,
            raw_model_output='{"ticket_id": "T-1", "priority": "high"}',
        )
    valid_so = structured.generate_structured(
        provider="Snowflake Proxy - Chat",
        model="openai-gpt-6-luna",
        input_message="create ticket",
        output_schema=schema,
        raw_model_output='{"ticket_id": "T-1", "priority": 1}',
    )
    assert valid_so["Structured Output"] == {"ticket_id": "T-1", "priority": 1}


def test_ai_function_component_redact_classify_scalar_vs_aggregate_and_cte_transport() -> None:
    ai_comp = SnowflakeAIFunctionComponent(
        active_role="ROLE_ANALYST",
        allowed_roles={"ROLE_ANALYST"},
    )
    sample_df = [
        {"id": 10, "comment": "Contact john.doe@example.com urgently"},
        {"id": 20, "comment": "Billing refund request for order 99"},
    ]

    # 1. Missing column_name rejected explicitly
    with pytest.raises(SnowflakeProviderError):
        ai_comp.execute(ai_function="AI_REDACT", table=sample_df, column_name=None)

    # 2. Passing Instructions to AI_REDACT is rejected (never silently ignored or converted to generative call)
    with pytest.raises(SnowflakeProviderError) as instr_exc:
        ai_comp.execute(
            ai_function="AI_REDACT",
            table=sample_df,
            column_name="comment",
            instructions="custom prompt not allowed on AI_REDACT",
        )
    assert instr_exc.value.code == ERROR_UNSUPPORTED_CAPABILITY

    # 3. AI_REDACT in `detect` mode masks raw PII span text in trace_payload [G-A2]
    redact_detect = ai_comp.execute(
        ai_function="AI_REDACT",
        table=sample_df,
        column_name="comment",
        redact_mode="detect",
        categories=["EMAIL", "NAME"],
    )
    assert redact_detect["result_granularity"] == "per_row"
    assert redact_detect["plan"].implicit_persistence is False
    assert redact_detect["plan"].transport_mode == "parameterized_values_cte"
    assert "SYSTEM$EXPLAIN_PRIVILEGES" in redact_detect["plan"].explain_privileges_sql
    assert "AI_COUNT_TOKENS" in redact_detect["plan"].token_estimate_sql
    assert redact_detect["disclaimer"] == AI_REDACT_LIMITATIONS_DISCLAIMER
    # Trace sink spans are masked
    trace_spans = redact_detect["trace_payload"]["rows"][0]["spans"]
    assert trace_spans[0]["text"] == "[MASKED_PII_SPAN]"

    # 4. AI_CLASSIFY requires non-empty categories and accepts optional instructions
    with pytest.raises(SnowflakeProviderError):
        ai_comp.execute(ai_function="AI_CLASSIFY", table=sample_df, column_name="comment", categories=[])

    classify_res = ai_comp.execute(
        ai_function="AI_CLASSIFY",
        table=sample_df,
        column_name="comment",
        categories=["support", "billing"],
        instructions="Classify customer inquiry topic",
    )
    assert classify_res["result_granularity"] == "per_row"
    assert classify_res["LLM Results"][1]["ai_result"]["labels"] == ["billing"]

    # 5. Aggregate function (AI_SUMMARIZE_AGG) returns aggregate result, not 1:1 per-row
    agg_res = ai_comp.execute(
        ai_function="AI_SUMMARIZE_AGG",
        table=sample_df,
        column_name="comment",
    )
    assert agg_res["result_granularity"] == "aggregate"
    assert len(agg_res["LLM Results"]) == 1
    assert agg_res["LLM Results"][0]["source_row_count"] == 2
