from __future__ import annotations
import json
from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MessageTextInput, Output, StrInput
from lfx.schema.message import Message
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

class SnowflakeKnowledgeMemoryComponent(Component):
    display_name = "Snowflake Knowledge & Memory"
    description = "Vector Memory & Knowledge Base backed by Standard Table (VECTOR), Hybrid Table (VECTOR), Cortex Search, or Snowflake Postgres (pgvector) with optional LLM Preprocessing."
    icon = "Snowflake"
    name = "SnowflakeKnowledgeMemory"

    inputs = [
        StrInput(name="memory_name", display_name="Memory Name", value="PROJECT_KNOWLEDGE_MEM"),
        DropdownInput(name="vector_backend", display_name="Vector Database Backend", options=["Standard Table (VECTOR)", "Hybrid Table (VECTOR)", "Cortex Search", "Snowflake Postgres (pgvector)"], value="Standard Table (VECTOR)"),
        DropdownInput(name="embedding_model", display_name="Snowflake / Ollama Embedding Model", options=["snowflake-arctic-embed-l-v2.0", "snowflake-arctic-embed-m-v1.5", "e5-base-v2", "ollama/nomic-embed-text"], value="snowflake-arctic-embed-m-v1.5"),
        IntInput(name="batch_size", display_name="Batch Size", value=64, advanced=True),
        StrInput(name="llm_preprocessing", display_name="Optional LLM Preprocessing Model", value="", advanced=True),
        MessageTextInput(name="query_or_text", display_name="Search Query or Chunk Text", value=""),
    ]
    outputs = [
        Output(display_name="Memory Result", name="memory_message", method="execute_memory"),
    ]

    def execute_memory(self) -> Message:
        safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in (self.memory_name or "MEM")).upper()
        table_fqn = f"{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.MEM_{safe_name}"
        return Message(text=json.dumps({"memory_name": self.memory_name, "table_fqn": table_fqn, "vector_backend": self.vector_backend, "embedding_model": self.embedding_model, "batch_size": self.batch_size, "llm_preprocessing": self.llm_preprocessing or None, "active_role": LIVE_BRIDGE.active_role}, indent=2))
