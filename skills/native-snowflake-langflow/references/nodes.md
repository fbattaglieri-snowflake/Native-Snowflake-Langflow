# Node reference

Generated from the node catalog of a running service (Langflow 1.13.1). Type names, field names and output
ports are the ones `Graph.add()` and `Graph.link()` expect. Check your own server with `lf.py node <Type>`:
field lists change between versions.

Reading the tables: **Field** is the name to use in `values`; **Accepts** lists what can be linked into the
field (empty means the field is typed in, not linked); hidden and advanced fields are left out unless they
are commonly needed. Dropdowns that list your Snowflake objects (database, schema, table, agent...) are filled
from the flow Context: pass the name you want as a plain string.

## Triggers

### `SnowflakeScheduleTrigger` · Schedule Trigger

Run this flow on a Snowflake schedule or Cron.

Outputs: `trigger_message_out` (Message), `execution_metadata` (JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `cron_expression` | `0 9 * * 1-5` |  |  |
| `schedule_type` | `Custom (Cron)` | `Custom (Cron)`, `Minutes`, `Hours`, `Days`, `Weeks`, `Months` |  |
| `status_mode` | `Active` | `Active`, `Paused` |  |
| `timezone` | from the Context |  |  |
| `trigger_message` | `Run scheduled workflow` |  | Message |

### `SnowflakeTableChangeTrigger` · Table Change Trigger

Trigger this flow when rows change in a Snowflake table

Outputs: `changed_rows` (Table), `trigger_message_out` (Message), `stream_metadata` (JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `database` | from the Context |  |  |
| `schema` | from the Context |  |  |
| `source_object` | from the Context |  |  |
| `status_mode` | `Active` | `Active`, `Paused` |  |
| `trigger_on` | `New Rows Only (Insert)` | `New Rows Only (Insert)`, `All Changes (Insert / Update / Delete)` |  |

### `SnowflakeStageFileTrigger` · Stage File Trigger

Trigger this flow when files arrive in a Snowflake Stage

Outputs: `staged_files` (Table), `trigger_message_out` (Message), `file_metadata` (JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `database` | from the Context |  |  |
| `file_pattern` | `*.pdf` |  |  |
| `schema` | from the Context |  |  |
| `stage_name` |  |  |  |
| `status_mode` | `Active` | `Active`, `Paused` |  |
| `trigger_on` | `New Files Only (Upload)` | `New Files Only (Upload)`, `New & Updated Files`, `All File Events (Upload / Update / Delete)` |  |

### `SnowflakeAlertTrigger` · Condition Alert Trigger

Trigger this flow when a Snowflake SQL condition is met

Outputs: `alert_rows` (Table), `trigger_message_out` (Message), `alert_metadata` (JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `alert_mode` | `Periodic Condition Check` | `Periodic Condition Check`, `Alert on New Data (Event-Driven)` |  |
| `check_frequency` | `Every 5 Minutes` | `Every 5 Minutes`, `Every 15 Minutes`, `Every 1 Hour`, `Custom (Cron)` |  |
| `condition_sql` | `SELECT ORDER_ID, TOTAL_AMOUNT, STATUS FROM SNOWFLAKE_LANG...` |  | Message |
| `status_mode` | `Active` | `Active`, `Paused` |  |

## Data

### `SnowflakeTables` · Tables

Governed Snowflake Tables Data Source (Standard Table, Hybrid Table, Interactive Table, Dynamic Table) with cascading Database -> Schema -> Table selection, input ports, and Read/Write Tool Mode for Agents.

Outputs: `data` (JSON), `dataframe` (Table)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `database` | from the Context |  |  |
| `operation_mode` | `SELECT (Read)` | `SELECT (Read)`, `INSERT / MERGE (Write)` |  |
| `query_filter_input` |  |  | Message |
| `rows_to_write` |  |  | Data, JSON (several) |
| `schema_name` | from the Context |  |  |
| `table_name` | from the Context |  |  |
| `table_subtype` | `Standard Table` | `Standard Table`, `Hybrid Table`, `Interactive Table`, `Dynamic Table` |  |

### `SnowflakeViews` · Views

Governed Snowflake Views Data Source (Standard View, Secure View, Materialized View) with cascading Database -> Schema -> View selection, input port, and Read Tool Mode for Agents.

Outputs: `data` (JSON), `dataframe` (Table)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `database` | from the Context |  |  |
| `query_filter_input` |  |  | Message |
| `schema_name` | from the Context |  |  |
| `view_name` | from the Context |  |  |
| `view_subtype` | `Standard View` | `Standard View`, `Secure View`, `Materialized View` |  |

### `SnowflakeIcebergTables` · Iceberg Tables

Governed Snowflake Apache Iceberg Tables Data Source (Snowflake-managed & Externally-managed with EXTERNAL_VOLUME and CATALOG) with cascading Database -> Schema -> Iceberg Table selection, input ports, and Read/Write Tool Mode for Agents.

Outputs: `data` (JSON), `dataframe` (Table)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `database` | from the Context |  |  |
| `iceberg_catalog_mode` | `Snowflake-managed` | `Snowflake-managed`, `Externally-managed` |  |
| `operation_mode` | `SELECT (Read)` | `SELECT (Read)`, `INSERT (Write)` |  |
| `query_filter_input` |  |  | Message |
| `rows_to_write` |  |  | Data, JSON (several) |
| `schema_name` | from the Context |  |  |
| `table_name` | from the Context |  |  |

### `SnowflakePostgres` · Postgres

Governed Snowflake Postgres Data Source (Account-Level Instance -> Postgres Database -> Postgres Schema -> Postgres Table cascading selection, Relational/JSONB/pgvector profiles, input ports, and Read/Write Tool Mode for Agents).

Outputs: `data` (JSON), `dataframe` (Table)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `operation_mode` | `SELECT (Read)` | `SELECT (Read)`, `INSERT / UPSERT (Write)` |  |
| `postgres_database` | `snowflake` |  |  |
| `postgres_instance` | from the Context |  |  |
| `postgres_schema` | `public` |  |  |
| `query_filter_input` |  |  | Message |
| `rows_to_write` |  |  | Data, JSON (several) |
| `storage_profile` | `Relational` | `Relational`, `JSONB`, `pgvector` |  |
| `table_name` | `langflow_pg_events` | `orders`, `customers`, `events` |  |

### `SnowflakeSQLStatement` · SQL Statement

Execute a governed Snowflake SQL statement (SELECT / CALL / SHOW) within context <Database>.<Schema> under the active role with DataFrame or JSON Object (OBJECT_CONSTRUCT) output.

Outputs: `dataframe` (Table), `message` (Message), `metadata` (JSON), `rows` (Message)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `bind_values` |  |  | Message, Data, JSON (several) |
| `database` | from the Context |  |  |
| `output_format` | `DataFrame` | `DataFrame`, `JSON Object (OBJECT_CONSTRUCT)` |  |
| `schema_name` | from the Context |  |  |
| `sql_statement` | `SELECT run_id, flow_id, effective_role, status FROM <DB>....` |  | Message |

### `SnowflakeCortexAnalyst` · Cortex Analyst

Natural-language-to-SQL via Snowflake Cortex Analyst (/api/v2/cortex/analyst/message) over an existing Semantic View (SHOW SEMANTIC VIEWS IN SCHEMA) or Stage YAML (LIST @DB.SCHEMA.STAGE) in the active role session, with Tool Mode for Agents.

Outputs: `message` (Message), `dataframe` (Table), `metadata` (JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `database` | from the Context |  |  |
| `execute_generated_sql` | `True` |  |  |
| `question` | `Show the latest flow runs by active role` |  | Message |
| `schema_name` | from the Context |  |  |
| `semantic_source` | from the Context |  |  |
| `source_mode` | `Semantic View` | `Semantic View`, `Stage YAML` |  |

### `SnowflakeCortexSearch` · Cortex Search

Query an existing Snowflake Cortex Search Service in <Database>.<Schema> (SHOW CORTEX SEARCH SERVICES IN SCHEMA) with hybrid lexical + vector retrieval, ATTRIBUTES filters (@eq, @contains, @and, @or, @not), limit, scoring_config, and Tool Mode for Agents.

Outputs: `message` (Message), `metadata` (JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `columns_csv` | `chunk,relative_path,page_number` |  |  |
| `database` | from the Context |  |  |
| `filter_json` |  |  | Message |
| `limit` | `5` |  |  |
| `query` | `How does Snowflake Langflow govern active roles on SPCS?` |  | Message |
| `schema_name` | from the Context |  |  |
| `scoring_config_json` |  |  | Message |
| `search_service` | from the Context |  |  |

## Models and agents

### `SnowflakeCortexAgent` · Cortex Agent

Invoke a Snowflake Cortex Agent (/api/v2/databases/{db}/schemas/{schema}/agents/{name}:run) via cascading Database -> Schema -> Cortex Agent selection with 2-level Snowflake Threads API stateful continuity and automatic parent_message_id recovery under CURR...

Outputs: `response` (Message), `structured_response` (Data/JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `conversation_thread` | from the Context |  |  |
| `cortex_agent` | `EXAMPLE_AGENT` |  |  |
| `database` | from the Context |  |  |
| `resume_thread` | `True` |  |  |
| `schema_name` | from the Context |  |  |
| `thread_continuity_mode` | `Resume Latest Thread of this Agent` | `Resume Latest Thread of this Agent`, `One Thread per Playground Session (Session ID)`, `Select Specific Thread / Start Fresh Thread` |  |
| `user_message` | `Summarize active flow metrics` |  | Message |

### `LanguageModelComponent` · Language Model

Runs a language model given a specified provider.

Outputs: `text_output` (Message), `model_output` (LanguageModel)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `input_value` |  |  | Message |
| `model` | `client.model_value("<model>")` |  | LanguageModel |
| `reasoning_effort` | `none` | `unspecified`, `none`, `low`, `medium`, `high` |  |
| `system_message` |  |  | Message |

### `Agent` · Agent

Define the agent's instructions, then enter a task to complete using tools.

Outputs: `response` (Message), `structured_response` (Data/JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `context_id` |  |  | Message |
| `format_instructions` | `You are an AI that extracts structured JSON objects from ...` |  | Message |
| `input_value` |  |  | Message |
| `model` | `client.model_value("<model>")` |  | LanguageModel |
| `output_schema` |  |  | DataFrame, Table |
| `reasoning_effort` | `none` | `unspecified`, `none`, `low`, `medium`, `high` |  |
| `system_prompt` | `You are a Langflow Agent — an AI assistant that completes...` |  | Message |
| `tools` |  |  | Tool, StructuredTool, BaseTool (several) |

### `SnowflakeAIComplete` · AI Complete

Dedicated Snowflake AI_COMPLETE generative component with capability-filtered model catalog (including gemini-3.1-pro, gemini-3.1-flash-lite, gemini-3.5-flash for .mp3/audio and video), model_parameters (temperature, top_p, max_tokens, guardrails), structur...

Outputs: `message` (Message), `details` (JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `batch_directory_mode` | `False` |  |  |
| `guardrails` | `False` |  |  |
| `max_tokens` | `1024` |  |  |
| `media_type_filter` | `All Compatible` | `All Compatible`, `Images (.jpg/.png/.webp/.gif/.bmp)`, `Audio (.mp3/.wav/.flac/.m4a/.ogg)`, `Video (.mp4/.mov/.webm/.mkv/.avi)` |  |
| `model` | from the Context |  |  |
| `prompt` | `Summarize the operational status of this workflow.` |  | Message |
| `response_format_mode` | `None (Text)` | `None (Text)`, `JSON Schema Object`, `Structured Decision Schema` |  |
| `stage_file_path` |  |  |  |
| `stage_selector` | from the Context |  |  |
| `temperature` | `0.0` |  |  |

## AI operations

### `SnowflakeAIFunction` · AI Function

Task-specific Snowflake Cortex AI_* Function execution across all 14 functions (AI_CLASSIFY, AI_FILTER, AI_EXTRACT, AI_SENTIMENT, AI_SUMMARIZE, AI_SUMMARIZE_AGG, AI_AGG, AI_TRANSLATE, AI_SIMILARITY, AI_TRANSCRIBE, AI_PARSE_DOCUMENT, AI_EMBED, AI_MULTI_EMBED...

Outputs: `message` (Message), `telemetry` (JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `ai_function` | from the Context |  |  |
| `batch_directory_mode` | `False` |  |  |
| `categories_or_schema` | `billing,support,technical` |  |  |
| `input_text` | `Evaluate customer feedback or document content.` |  | Message |
| `media_type_filter` | `All Compatible` | `All Compatible`, `Images (.jpg/.png/.webp/.gif/.bmp)`, `Audio (.mp3/.wav/.flac/.m4a/.ogg)`, `Video (.mp4/.mov/.webm/.mkv/.avi)`, `Documents (.pdf/.docx/.pptx)` |  |
| `return_as_json` | `False` |  |  |
| `second_text_or_stage_file` |  |  |  |
| `stage_file_path` |  |  |  |
| `stage_selector` | from the Context |  |  |

### `SnowflakeAIRedact` · AI Redact

Dedicated Snowflake Cortex AI_REDACT component supporting all 13 official PII categories, 'redact', 'detect', and 'detect_with_allowlist' (selective reverse-index span splicing) modes, multi-row error handling (AI_SQL_ERROR_HANDLING_USE_FAIL_ON_ERROR = FALS...

Outputs: `message` (Message), `telemetry` (JSON)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `auto_chunk_long_text` | `False` |  |  |
| `input_text` | `Contact Mario Rossi at mario.rossi@example.com or +39 02 ...` |  | Message |
| `pii_categories` | `ALL` |  |  |
| `redact_mode` | `redact` | `redact`, `detect`, `detect_with_allowlist` |  |
| `return_error_details` | `True` |  |  |
| `source_mode` | `Text Input` | `Text Input`, `Table Column`, `Stage Document (AI_PARSE_DOCUMENT -> AI_REDACT)`, `Stage Audio/Video (AI_TRANSCRIBE -> AI_REDACT)` |  |

### `SnowflakeDecision` · Decision

Official Snowflake Decision component powered by AI_COMPLETE with 'snowflake-decision-fast' (low latency, 2..16 choice options, 32k token limit) and 'snowflake-decision' (high accuracy, 2..32 choice options, 256 KiB limit), supporting choice, score (0..n-1)...

Outputs: `decision_object` (JSON), `high_confidence_route` (JSON), `low_confidence_route` (JSON), `summary` (Message), `table_decisions` (Table)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `confidence_threshold` | `0.7` |  |  |
| `evaluation_mode` | `Single State (Text / JSON Input)` | `Single State (Text / JSON Input)`, `Table-Level Processing (Database.Schema.Table)` |  |
| `include_none_of_these` | `True` |  |  |
| `model` | `snowflake-decision-fast` | `snowflake-decision-fast`, `snowflake-decision` |  |
| `noul_threshold` | `0.7` |  |  |
| `question_criteria_json` | `{   "billing": "Payment, invoice, or subscription issues"...` |  | Message |
| `question_id` | `department` |  |  |
| `question_instructions` | `Which team should handle this?` |  | Message |
| `question_type` | `choice` | `choice`, `score`, `noul` |  |
| `questions_json` | `{   "department": {     "type": "choice",     "instructio...` |  | Message |
| `return_error_details` | `True` |  |  |
| `state_input` | `Our production billing pipeline failed after invoice #42 ...` |  | Message |

### `SnowflakeCustomTool` · Custom Tool

Unified Snowflake Custom Tool mirroring Snowsight 'Add custom tool' 1:1 for Stored Procedures ('procedure'), Scalar Functions ('function'), and Tabular Functions ('table_function' / UDTF) with signature auto-discovery, parameter schema sorting/editing, Ware...

Outputs: `tool` (JSON), `result` (Message)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `custom_tool_identifier` |  |  |  |
| `database_schema` | from the Context |  |  |
| `invocation_arguments` | `{"body": "Deployment succeeded", "recipient_email": "team...` |  |  |
| `parameters_json` | `[]` |  | Message |
| `parameters_sort_by` | `Latest added` | `Latest added`, `Alphabetical`, `Required first` |  |
| `query_timeout` | `60` |  |  |
| `resource_type` | `procedure` | `procedure`, `function`, `table_function` |  |
| `tool_description` | `Send an email notification using the governed Snowflake p...` |  | Message |
| `tool_name` | `send_email_all` |  |  |
| `warehouse_mode` | `User's default` | `User's default`, `Custom` |  |

## Memory

### `SnowflakeAgentMemory` · Agent Memory

Read and write a Snowflake memory created from the Memories page: checkpoint the state of a run, keep a running summary, store a user profile, or save and recall past episodes. The memory is a table in the flow context; in Tool Mode an agent decides when to...

Outputs: `result` (JSON), `text` (Message)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `content` |  |  | Message |
| `key` |  |  | Message |
| `memory` | `None` | `None` |  |
| `operation` | `Recall` | `Recall`, `Save`, `Clean up` |  |
| `outcome_score` | `1.0` |  |  |
| `query` |  |  | Message |
| `topic` |  |  | Message |

### `MessageHistory` · Message History

Store or retrieve #2 Short-Term Buffer / Window Memory chat turns on Account-Level Snowflake Postgres (:5432, public.chat_message_history) or Snowflake Hybrid / Standard Tables (<DB>.<SCHEMA>) with window_k sliding window, session_id isolation, External Mem...

Outputs: `messages` (Message), `table` (Data/DataFrame/JSON), `replay_state` (Data/JSON), `stored_message` (Message)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `conversation` |  |  |  |
| `filter_by_session` | `True` |  |  |
| `memory` |  |  | Memory, BaseChatMessageHistory (several) |
| `memory_base` | `None` | `None` |  |
| `message_text` |  |  | Message |
| `mode` | `Retrieve` | `Retrieve`, `Store` |  |
| `pg_table` | `public.chat_message_history` |  |  |
| `postgres_instance` | `LANGFLOW_PG_INSTANCE` |  |  |
| `session_id` |  |  |  |
| `storage_family` | `snowflake_postgres` | `snowflake_postgres`, `snowflake_sql` |  |
| `window_k` | `8` |  |  |

## Flow plumbing (standard Langflow)

### `ChatInput` · Chat Input

Get chat inputs from the Playground.

Outputs: `message` (Message)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `context_id` |  |  | Message |
| `input_value` |  |  |  |
| `sender_name` | `User` |  | Message |
| `session_id` |  |  | Message |

### `ChatOutput` · Chat Output

Display a chat message in the Playground.

Outputs: `message` (Message)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `context_id` |  |  | Message |
| `data_template` | `{text}` |  | Message |
| `input_value` |  |  | Data, JSON, DataFrame, Table, Message |
| `sender_name` | `AI` |  | Message |
| `session_id` |  |  | Message |

### `TextOutput` · Text Output

Sends text output via API.

Outputs: `text` (Message)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `input_value` |  |  | Message |

### `Prompt Template` · Prompt Template

Create a prompt template with dynamic variables.

Outputs: `prompt` (Message)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `template` |  |  |  |

### `ConditionalRouter` · If-Else

Routes an input message to a corresponding output based on text comparison.

Outputs: `true_result` (Message), `false_result` (Message)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `false_case_message` |  |  | Message |
| `input_text` |  |  | Message |
| `match_text` |  |  | Message |
| `operator` | `equals` | `equals`, `not equals`, `contains`, `starts with`, `ends with`, `regex`, `less than`, `less than or equal`, `greater than`, `greater than or equal` |  |
| `true_case_message` |  |  | Message |

### `TypeConverterComponent` · Type Convert

Convert between different types (Message, JSON, Table)

Outputs: `message_output` (Message), `data_output` (JSON), `dataframe_output` (Table)

| Field | Default | Choices | Accepts |
|---|---|---|---|
| `input_data` |  |  | Message, Data, JSON, DataFrame, Table |
| `output_type` | `Message` | `Message`, `JSON`, `Table` |  |
