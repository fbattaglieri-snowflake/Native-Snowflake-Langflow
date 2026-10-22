from __future__ import annotations

import json
from typing import Any

from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MessageTextInput, Output, StrInput
from lfx.schema.message import Message
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


OFFICIAL_AI_EMBED_DIMENSIONS: dict[str, int] = {
    "snowflake-arctic-embed-m-v1.5": 768,
    "snowflake-arctic-embed-m": 768,
    "e5-base-v2": 768,
    "nomic-embed-text": 768,
    "ollama/nomic-embed-text": 768,
    "snowflake-arctic-embed-l-v2.0": 1024,
    "snowflake-arctic-embed-l-v2.0-8k": 1024,
    "multilingual-e5-large": 1024,
    "voyage-multilingual-2": 1024,
    "nv-embed-qa-4": 1024,
    "voyage-multimodal-3": 1024,
}


def _database_options() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    dbs = list(ctx.get("databases") or [])
    default_db = getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
    if default_db and default_db not in dbs:
        dbs.insert(0, default_db)
    return dbs or ["SNOWFLAKE_LANGFLOW"]


def _schema_options(database: str | None = None) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    db = (database or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
    schemas = list((ctx.get("schemas_by_db") or {}).get(db) or [])
    default_schema = getattr(LIVE_BRIDGE, "schema", "CORE")
    if default_schema and default_schema not in schemas:
        schemas.insert(0, default_schema)
    return schemas or ["CORE", "PUBLIC"]


class SnowflakeKnowledgeMemoryComponent(Component):
    display_name = "Snowflake Knowledge & Memory"
    description = (
        "Governed Knowledge & Memory backed by Vector Memory (Standard/Hybrid/Interactive/Dynamic VECTOR, "
        "Cortex Search Service, Snowflake Postgres pgvector), Structured Table Memory (Standard/Hybrid/Interactive/Dynamic), "
        "Semi-Structured (VARIANT/OBJECT/ARRAY), or Snowflake Postgres (Relational/JSONB) with official AI_EMBED models."
    )
    icon = "Snowflake"
    name = "SnowflakeKnowledgeMemory"

    inputs = [
        DropdownInput(
            name="database",
            display_name="1. Database",
            options=_database_options(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="2. Schema",
            options=_schema_options("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="memory_name",
            display_name="Memory Name",
            value="PROJECT_KNOWLEDGE_MEM",
        ),
        DropdownInput(
            name="memory_category",
            display_name="Memory Category",
            options=[
                "Vector Memory",
                "Structured Table Memory",
                "Semi-Structured (VARIANT/OBJECT/ARRAY)",
                "Snowflake Postgres (Relational/JSONB)",
                "Snowflake Postgres (pgvector)",
            ],
            value="Vector Memory",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="table_type",
            display_name="Table Type",
            options=["STANDARD", "HYBRID", "INTERACTIVE", "DYNAMIC"],
            value="STANDARD",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="semi_structured_type",
            display_name="Semi-Structured Column Type",
            options=["VARIANT", "OBJECT", "ARRAY"],
            value="VARIANT",
            advanced=True,
        ),
        DropdownInput(
            name="vector_backend",
            display_name="Vector Database Backend",
            options=[
                "Standard Table (VECTOR)",
                "Hybrid Table (VECTOR)",
                "Interactive Table (VECTOR)",
                "Dynamic Table (VECTOR)",
                "Cortex Search Service",
                "Cortex Search",
                "Snowflake Postgres (pgvector)",
            ],
            value="Standard Table (VECTOR)",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="embedding_model",
            display_name="Snowflake AI_EMBED / Ollama Embedding Model",
            options=list(OFFICIAL_AI_EMBED_DIMENSIONS.keys()),
            value="snowflake-arctic-embed-m-v1.5",
            real_time_refresh=True,
        ),
        IntInput(
            name="batch_size",
            display_name="Batch Size",
            value=64,
            advanced=True,
        ),
        StrInput(
            name="llm_preprocessing",
            display_name="Optional LLM Preprocessing Model",
            value="",
            advanced=True,
        ),
        MessageTextInput(
            name="query_or_text",
            display_name="Search Query or Chunk Text",
            value="",
        ),
    ]
    outputs = [
        Output(display_name="Memory Result", name="memory_message", method="execute_memory"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _database_options()
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _schema_options(db_val)
        return build_config

    def execute_memory(self) -> Message:
        db = (getattr(self, "database", "") or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
        sch = (getattr(self, "schema_name", "") or getattr(LIVE_BRIDGE, "schema", "CORE")).strip()
        safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in (self.memory_name or "MEM")).upper()
        table_fqn = f"{db}.{sch}.MEM_{safe_name}"
        category = (getattr(self, "memory_category", "Vector Memory") or "Vector Memory").strip()
        tbl_type = (getattr(self, "table_type", "STANDARD") or "STANDARD").strip().upper()
        semi_type = (getattr(self, "semi_structured_type", "VARIANT") or "VARIANT").strip().upper()
        emb_model = (getattr(self, "embedding_model", "snowflake-arctic-embed-m-v1.5") or "snowflake-arctic-embed-m-v1.5").strip()
        dim = OFFICIAL_AI_EMBED_DIMENSIONS.get(emb_model, 768)

        if category == "Structured Table Memory":
            ddl_preview = (
                f"CREATE {tbl_type + ' ' if tbl_type != 'STANDARD' else ''}TABLE IF NOT EXISTS {table_fqn} ("
                f"id VARCHAR(128) PRIMARY KEY, session_id VARCHAR(128), role VARCHAR(64), "
                f"content VARCHAR, created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )
        elif category == "Semi-Structured (VARIANT/OBJECT/ARRAY)":
            ddl_preview = (
                f"CREATE {tbl_type + ' ' if tbl_type != 'STANDARD' else ''}TABLE IF NOT EXISTS {table_fqn} ("
                f"id VARCHAR(128) PRIMARY KEY, payload {semi_type}, metadata VARIANT, "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )
        elif category == "Snowflake Postgres (Relational/JSONB)":
            ddl_preview = (
                f"-- Snowflake Postgres (:5432) Relational / JSONB Memory\n"
                f"CREATE TABLE IF NOT EXISTS mem_{safe_name.lower()} ("
                f"id VARCHAR(128) PRIMARY KEY, session_id VARCHAR(128), content TEXT, "
                f"payload JSONB DEFAULT '{{}}'::jsonb, created_at TIMESTAMPTZ DEFAULT NOW());"
            )
        elif category == "Snowflake Postgres (pgvector)" or self.vector_backend == "Snowflake Postgres (pgvector)":
            ddl_preview = (
                f"-- Snowflake Postgres (:5432) pgvector Memory\n"
                f"CREATE EXTENSION IF NOT EXISTS vector;\n"
                f"CREATE TABLE IF NOT EXISTS mem_{safe_name.lower()} ("
                f"id VARCHAR(128) PRIMARY KEY, content TEXT, metadata JSONB, "
                f"embedding vector({dim}), created_at TIMESTAMPTZ DEFAULT NOW());"
            )
        else:
            ddl_preview = (
                f"CREATE {tbl_type + ' ' if tbl_type != 'STANDARD' else ''}TABLE IF NOT EXISTS {table_fqn} ("
                f"id VARCHAR(128) PRIMARY KEY, content VARCHAR, metadata VARIANT, "
                f"embedding VECTOR(FLOAT, {dim}), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )

        payload = {
            "database": db,
            "schema_name": sch,
            "memory_name": self.memory_name,
            "table_fqn": table_fqn,
            "memory_category": category,
            "table_type": tbl_type,
            "semi_structured_type": semi_type,
            "vector_backend": self.vector_backend,
            "embedding_model": emb_model,
            "vector_dimension": dim,
            "batch_size": self.batch_size,
            "llm_preprocessing": self.llm_preprocessing or None,
            "ddl_preview": ddl_preview,
            "active_role": LIVE_BRIDGE.active_role,
        }
        return Message(text=json.dumps(payload, indent=2))
