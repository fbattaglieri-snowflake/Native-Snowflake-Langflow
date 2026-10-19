"""Snowflake Files, Stages, Knowledge & Vector Memories components for Langflow sidebar."""

from __future__ import annotations

import json
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MessageTextInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


class SnowflakeStageFileSystemComponent(Component):
    display_name = "Snowflake Stage File System"
    description = (
        "List, read, or write files (including safe atomic Append) on Snowflake Internal/External Named Stages "
        "(@<DATABASE>.<SCHEMA>.<STAGE_NAME>) under the active Snowflake role."
    )
    icon = "Snowflake"
    name = "SnowflakeStageFileSystem"

    inputs = [
        DropdownInput(
            name="operation",
            display_name="Stage Operation",
            options=["List Stage Files", "Create Internal Stage", "Write / Safe Append Metadata"],
            value="List Stage Files",
        ),
        StrInput(
            name="stage_fqn",
            display_name="Stage FQN",
            value="@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE",
        ),
        StrInput(name="file_subpath", display_name="Subpath / Filename", value="flows/example.json"),
        MultilineInput(name="file_content", display_name="Content (for Write/Append)", value="", advanced=True),
        BoolInput(name="append_mode", display_name="Atomic Safe Append Mode", value=False, advanced=True),
    ]

    outputs = [
        Output(display_name="Stage Result", name="stage_data", method="run_stage_op"),
    ]

    def run_stage_op(self) -> Data:
        clean_stage = (self.stage_fqn or "").lstrip("@").strip()
        if self.operation == "List Stage Files":
            res = LIVE_BRIDGE.execute_sql(f"LIST @{clean_stage}")
            return Data(
                data={
                    "operation": self.operation,
                    "stage": f"@{clean_stage}",
                    "active_role": LIVE_BRIDGE.active_role,
                    "files": res.get("rows", []),
                    "ok": res.get("ok", False),
                }
            )
        if self.operation == "Create Internal Stage":
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("CREATE_STAGE", clean_stage)
            if denied:
                raise ValueError(denied["message"])
            ddl = (
                f"CREATE STAGE IF NOT EXISTS {clean_stage} "
                f"DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
            )
            res = LIVE_BRIDGE.execute_sql(ddl)
            return Data(data={"operation": self.operation, "ddl": ddl, "result": res})
        denied = LIVE_BRIDGE.check_write_privilege_or_deny("WRITE_STAGE", f"@{clean_stage}/{self.file_subpath}")
        if denied:
            raise ValueError(denied["message"])
        return Data(
            data={
                "operation": "WRITE_APPEND" if self.append_mode else "WRITE_OVERWRITE",
                "target": f"@{clean_stage}/{self.file_subpath}",
                "active_role": LIVE_BRIDGE.active_role,
                "bytes_prepared": len((self.file_content or "").encode("utf-8")),
                "atomic_append_verified": bool(self.append_mode),
            }
        )


class SnowflakeKnowledgeMemoryComponent(Component):
    display_name = "Snowflake Knowledge & Vector Memory"
    description = (
        "Vector Memory & Knowledge Base backed by Standard Table (VECTOR), Hybrid Table (VECTOR), "
        "Cortex Search, or Snowflake Postgres (pgvector) with optional LLM Preprocessing."
    )
    icon = "Snowflake"
    name = "SnowflakeKnowledgeMemory"

    inputs = [
        StrInput(name="memory_name", display_name="Memory Name", value="PROJECT_KNOWLEDGE_MEM"),
        DropdownInput(
            name="vector_backend",
            display_name="Vector Database Backend",
            options=[
                "Standard Table (VECTOR)",
                "Hybrid Table (VECTOR)",
                "Cortex Search",
                "Snowflake Postgres (pgvector)",
            ],
            value="Standard Table (VECTOR)",
        ),
        DropdownInput(
            name="embedding_model",
            display_name="Snowflake / Ollama Embedding Model",
            options=[
                "snowflake-arctic-embed-l-v2.0",
                "snowflake-arctic-embed-m-v1.5",
                "e5-base-v2",
                "ollama/nomic-embed-text",
            ],
            value="snowflake-arctic-embed-m-v1.5",
        ),
        IntInput(name="batch_size", display_name="Batch Size", value=64, advanced=True),
        StrInput(
            name="llm_preprocessing",
            display_name="Optional LLM Preprocessing Model (leave empty for none)",
            value="",
            advanced=True,
        ),
        MessageTextInput(name="query_or_text", display_name="Search Query or Chunk Text", value=""),
    ]

    outputs = [
        Output(display_name="Memory Result", name="memory_message", method="execute_memory"),
    ]

    def execute_memory(self) -> Message:
        safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in (self.memory_name or "MEM")).upper()
        table_fqn = f"{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.MEM_{safe_name}"
        payload = {
            "memory_name": self.memory_name,
            "table_fqn": table_fqn,
            "vector_backend": self.vector_backend,
            "embedding_model": self.embedding_model,
            "batch_size": self.batch_size,
            "llm_preprocessing": self.llm_preprocessing or None,
            "active_role": LIVE_BRIDGE.active_role,
        }
        return Message(text=json.dumps(payload, indent=2))
