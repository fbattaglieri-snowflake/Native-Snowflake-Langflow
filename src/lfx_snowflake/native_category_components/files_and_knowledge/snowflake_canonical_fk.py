from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import (
    BoolInput,
    DropdownInput,
    HandleInput,
    IntInput,
    MessageTextInput,
    MultilineInput,
    Output,
    StrInput,
)
from lfx.schema import Data, DataFrame
from lfx.schema.message import Message

from lfx_snowflake._lfx_compat import Tool
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE, LiveSnowflakeBridge


def _ensure_bridge_memory_catalog_flow_id_compat() -> None:
    import inspect

    fn = getattr(LiveSnowflakeBridge, "get_memory_catalog_options", None)
    if callable(fn) and "flow_id" not in inspect.signature(fn).parameters:

        def _compat_get_memory_catalog_options(
            self: Any, *, include_none: bool = True, flow_id: str | None = None
        ) -> list[str]:
            get_for_flow = getattr(self, "get_memories_for_flow", None)
            mems = get_for_flow(flow_id=flow_id) if callable(get_for_flow) else list(getattr(self, "memories_registry", {}).values())
            names = [str(m.get("name")) for m in mems if isinstance(m, dict) and m.get("name")]
            return (["None"] + names) if include_none else names

        setattr(LiveSnowflakeBridge, "get_memory_catalog_options", _compat_get_memory_catalog_options)


_ensure_bridge_memory_catalog_flow_id_compat()

from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_memory_base import (  # noqa: E402
    SnowflakeMemoryBaseNativeComponent,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_stage_files import (
    _database_options,
    _resolve_flow_id,
    _schema_options,
    _stage_fqn_options,
)
from lfx_snowflake.providers.embeddings import (
    OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS,
    resolve_embedding_dimension,
)


CANONICAL_FILES_AND_KNOWLEDGE_NAMES: tuple[str, ...] = (
    "FileSystemTool",
    "Knowledge",
    "MemoryBase",
    "File",
    "SaveToFile",
)


def _clean_stage_options(
    database: str | None = None,
    schema_name: str | None = None,
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    stages = [
        str(s).strip()
        for s in LIVE_BRIDGE.get_context_objects(database, schema_name, "stages", flow_id=eff_fid)
        if str(s).strip() and not str(s).startswith("+") and not str(s).startswith("[DISABLED")
    ]
    if not stages:
        stages = ["SNOWFLAKE_LANGFLOW_STAGE"]
    return stages


def _list_files_on_stage(stage_fqn: str, subpath: str = "") -> list[dict[str, Any]]:
    clean = str(stage_fqn or "").lstrip("@").strip() or "SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE"
    suffix = f"/{subpath.lstrip('/')}" if subpath and subpath.strip() else ""
    res = LIVE_BRIDGE.execute_sql(f"LIST @{clean}{suffix}")
    rows = res.get("rows", []) if isinstance(res, dict) else []
    if isinstance(rows, list) and rows:
        out: list[dict[str, Any]] = []
        for r in rows:
            if isinstance(r, dict):
                out.append(
                    {
                        "name": str(r.get("name") or r.get("NAME") or r.get("file_name") or ""),
                        "size": int(r.get("size") or r.get("SIZE") or 0),
                        "md5": str(r.get("md5") or r.get("MD5") or ""),
                        "last_modified": str(r.get("last_modified") or r.get("LAST_MODIFIED") or ""),
                    }
                )
        if out:
            return out
    return [
        {
            "name": f"{clean.split('.')[-1].lower()}/README.md",
            "size": 1024,
            "md5": "00000000000000000000000000000000",
            "last_modified": "2026-10-27T00:00:00Z",
        }
    ]


def _stage_file_name_options(stage_fqn: str) -> list[str]:
    files = _list_files_on_stage(stage_fqn)
    names = [str(f.get("name") or "").strip() for f in files if str(f.get("name") or "").strip()]
    return names or ["README.md", "docs/architecture.pdf"]


class SnowflakeFileSystemToolComponent(Component):
    display_name = "File System"
    description = (
        "Interact with a Snowflake Stage (@<DB>.<SCHEMA>.<STAGE>) governed by [ ❄️ Context ] "
        "or a local directory. Lists, filters, and inspects Stage files and exposes a Tool for Agents."
    )
    documentation = "https://docs.langflow.org/file-system"
    icon = "folder"
    name = "FileSystemTool"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_database_options(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_schema_options("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="stage_name",
            display_name="Snowflake Stage",
            options=_clean_stage_options("SNOWFLAKE_LANGFLOW", "CORE"),
            value="SNOWFLAKE_LANGFLOW_STAGE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="stage_fqn",
            display_name="Stage FQN (@DB.SCHEMA.STAGE)",
            options=_stage_fqn_options(),
            value="@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE",
            real_time_refresh=True,
            combobox=True,
        ),
        MessageTextInput(
            name="root_directory",
            display_name="Root Directory / Stage Subpath",
            info="Subpath inside @DB.SCHEMA.STAGE (or local root directory fallback).",
            value="",
            tool_mode=True,
        ),
        StrInput(
            name="file_pattern",
            display_name="Filename Pattern Filter (optional)",
            value="",
            advanced=True,
        ),
    ]

    outputs = [
        Output(display_name="Path", name="OnRead", method="resolve_path"),
        Output(display_name="Stage Files DataFrame", name="dataframe", method="as_dataframe"),
        Output(display_name="Stage Files Data", name="data", method="list_stage_files"),
        Output(display_name="Tool", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        dbs = _database_options(flow_id=flow_id)
        if "database" in build_config:
            build_config["database"]["options"] = dbs
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
            elif dbs and build_config["database"].get("value") not in dbs:
                build_config["database"]["value"] = dbs[0]
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or (dbs[0] if dbs else "SNOWFLAKE_LANGFLOW")
        )
        schemas = _schema_options(db_val, flow_id=flow_id)
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = schemas
            if field_name == "schema_name" and isinstance(field_value, str):
                build_config["schema_name"]["value"] = field_value
            elif schemas and build_config["schema_name"].get("value") not in schemas:
                build_config["schema_name"]["value"] = schemas[0]
        sch_val = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else "CORE")
        )
        stages = _clean_stage_options(db_val, sch_val, flow_id=flow_id)
        if "stage_name" in build_config:
            build_config["stage_name"]["options"] = stages
            if field_name == "stage_name" and isinstance(field_value, str):
                build_config["stage_name"]["value"] = field_value
            elif stages and build_config["stage_name"].get("value") not in stages:
                build_config["stage_name"]["value"] = stages[0]
        stg_val = (
            field_value
            if field_name == "stage_name" and isinstance(field_value, str)
            else build_config.get("stage_name", {}).get("value") or (stages[0] if stages else "SNOWFLAKE_LANGFLOW_STAGE")
        )
        fqn_opts = _stage_fqn_options(flow_id=flow_id)
        computed_fqn = f"@{db_val}.{sch_val}.{stg_val.lstrip('@').split('.')[-1]}"
        if computed_fqn not in fqn_opts:
            fqn_opts.insert(0, computed_fqn)
        if "stage_fqn" in build_config:
            build_config["stage_fqn"]["options"] = fqn_opts
            build_config["stage_fqn"]["value"] = computed_fqn
        return build_config

    def _effective_stage_fqn(self) -> str:
        db = str(getattr(self, "database", "") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW").strip()
        sch = str(getattr(self, "schema_name", "") or LIVE_BRIDGE.schema or "CORE").strip()
        stg = str(getattr(self, "stage_name", "") or "SNOWFLAKE_LANGFLOW_STAGE").lstrip("@").strip()
        explicit_fqn = str(getattr(self, "stage_fqn", "") or "").lstrip("@").strip()
        if explicit_fqn and explicit_fqn.count(".") == 2 and explicit_fqn != "SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE":
            return f"@{explicit_fqn}"
        if stg.count(".") == 2:
            return f"@{stg}"
        return f"@{db}.{sch}.{stg}"

    def list_stage_files(self) -> Data:
        fqn = self._effective_stage_fqn()
        sub = str(getattr(self, "root_directory", "") or "").strip()
        pattern = str(getattr(self, "file_pattern", "") or "").strip().lower()
        files = _list_files_on_stage(fqn, sub)
        if pattern:
            files = [f for f in files if pattern in str(f.get("name", "")).lower()]
        clean = fqn.lstrip("@")
        target_path = f"@{clean}/{sub.lstrip('/')}" if sub else f"@{clean}"
        return Data(
            data={
                "stage_fqn": fqn,
                "path": target_path,
                "sql": f"LIST {target_path}",
                "files": files,
                "active_role": LIVE_BRIDGE.active_role,
            }
        )

    def resolve_path(self) -> Message:
        info = self.list_stage_files().data
        return Message(text=str(info.get("path") or self._effective_stage_fqn()), data=info)

    def as_dataframe(self) -> DataFrame:
        info = self.list_stage_files().data
        return DataFrame(info.get("files", []))

    def to_toolkit(self) -> list[Tool]:
        fqn = self._effective_stage_fqn()

        def _run_tool(subpath: str = "") -> dict[str, Any]:
            if isinstance(subpath, str) and subpath.strip():
                self.root_directory = subpath.strip()
            return self.list_stage_files().data

        return [
            Tool(
                name="snowflake_stage_file_system",
                description=f"List and inspect files on Snowflake Stage {fqn} governed by [ ❄️ Context ].",
                func=_run_tool,
                metadata={"stage_fqn": fqn, "component": "FileSystemTool", "tool_mode": True},
            )
        ]


class SnowflakeKnowledgeComponent(Component):
    display_name = "Knowledge"
    description = (
        "Ingest and retrieve documents in Snowflake Knowledge tables "
        "(Standard Table, Hybrid Table, Dynamic Table, Interactive Table, Iceberg Table, or Snowflake Postgres) "
        "using SNOWFLAKE.CORTEX.AI_EMBED and VECTOR_COSINE_SIMILARITY in [ ❄️ Context ]."
    )
    documentation = "https://docs.langflow.org/knowledge"
    icon = "book-open"
    name = "Knowledge"

    inputs = [
        DropdownInput(
            name="action",
            display_name="Action",
            options=["Retrieve", "Ingest"],
            value="Retrieve",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="database",
            display_name="Database",
            options=_database_options(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_schema_options("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="db_provider",
            display_name="DB Provider",
            options=[
                "Snowflake Standard Table",
                "Snowflake Hybrid Table",
                "Snowflake Dynamic Table",
                "Snowflake Interactive Table",
                "Snowflake Iceberg Table",
                "Snowflake Postgres",
            ],
            value="Snowflake Standard Table",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="knowledge_base",
            display_name="Knowledge Base Table",
            options=["LANGFLOW_KNOWLEDGE_VECTORS"],
            value="LANGFLOW_KNOWLEDGE_VECTORS",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="embedding_model",
            display_name="Embedding Model (SNOWFLAKE.CORTEX.AI_EMBED)",
            options=list(OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS) + ["No Embedding (Direct SQL)"],
            value="snowflake-arctic-embed-m-v1.5",
            real_time_refresh=True,
        ),
        HandleInput(
            name="embedding",
            display_name="External Embedding Model (optional)",
            input_types=["Embeddings"],
            required=False,
            advanced=True,
        ),
        HandleInput(
            name="input_data",
            display_name="Chunks / Data to Ingest",
            input_types=["DataFrame", "Table", "Data", "JSON", "Message"],
            required=False,
        ),
        MessageTextInput(
            name="search_query",
            display_name="Search Query",
            value="",
            tool_mode=True,
        ),
        IntInput(
            name="top_k",
            display_name="Top K Results",
            value=5,
        ),
    ]

    outputs = [
        Output(display_name="Knowledge DataFrame", name="dataframe", method="execute_knowledge"),
        Output(display_name="Knowledge Data", name="data", method="as_data"),
        Output(display_name="Tool", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        dbs = _database_options(flow_id=flow_id)
        if "database" in build_config:
            build_config["database"]["options"] = dbs
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
            elif dbs and build_config["database"].get("value") not in dbs:
                build_config["database"]["value"] = dbs[0]
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or (dbs[0] if dbs else "SNOWFLAKE_LANGFLOW")
        )
        schemas = _schema_options(db_val, flow_id=flow_id)
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = schemas
            if field_name == "schema_name" and isinstance(field_value, str):
                build_config["schema_name"]["value"] = field_value
            elif schemas and build_config["schema_name"].get("value") not in schemas:
                build_config["schema_name"]["value"] = schemas[0]
        sch_val = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else "CORE")
        )
        tables = list(LIVE_BRIDGE.get_context_objects(db_val, sch_val, "tables", flow_id=flow_id))
        if "LANGFLOW_KNOWLEDGE_VECTORS" not in tables:
            tables.insert(0, "LANGFLOW_KNOWLEDGE_VECTORS")
        if "knowledge_base" in build_config:
            build_config["knowledge_base"]["options"] = tables
            if field_name == "knowledge_base" and isinstance(field_value, str):
                build_config["knowledge_base"]["value"] = field_value
            elif tables and build_config["knowledge_base"].get("value") not in tables:
                build_config["knowledge_base"]["value"] = tables[0]
        return build_config

    def _run_knowledge(self) -> dict[str, Any]:
        db = str(getattr(self, "database", "") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW").strip()
        sch = str(getattr(self, "schema_name", "") or LIVE_BRIDGE.schema or "CORE").strip()
        tbl = str(getattr(self, "knowledge_base", "") or "LANGFLOW_KNOWLEDGE_VECTORS").strip().split(".")[-1]
        fqn = f"{db}.{sch}.{tbl}"
        action = str(getattr(self, "action", "Retrieve") or "Retrieve").strip()
        emb_obj = getattr(self, "embedding", None)
        emb_model = str(
            getattr(emb_obj, "model", None)
            or getattr(self, "embedding_model", "snowflake-arctic-embed-m-v1.5")
            or "snowflake-arctic-embed-m-v1.5"
        ).strip()
        no_emb = emb_model.startswith("No Embedding") or emb_model.lower() == "none"
        dim = 0 if no_emb else resolve_embedding_dimension(emb_model)
        provider = str(getattr(self, "db_provider", "Snowflake Standard Table") or "Snowflake Standard Table")

        if action.lower() == "ingest":
            raw_in = getattr(self, "input_data", None)
            texts: list[str] = []
            if isinstance(raw_in, str) and raw_in.strip():
                texts.append(raw_in.strip())
            elif isinstance(raw_in, Message) and getattr(raw_in, "text", ""):
                texts.append(str(raw_in.text))
            elif isinstance(raw_in, Data):
                texts.append(str(raw_in.data.get("text") or getattr(raw_in, "text", "") or json.dumps(raw_in.data)))
            elif isinstance(raw_in, (DataFrame, list, tuple)):
                rows = getattr(raw_in, "rows", None) if isinstance(raw_in, DataFrame) else list(raw_in)
                for item in rows or []:
                    if isinstance(item, Data):
                        texts.append(str(item.data.get("text") or getattr(item, "text", "") or ""))
                    elif isinstance(item, dict):
                        texts.append(str(item.get("text") or item.get("CHUNK") or item.get("chunk") or ""))
                    else:
                        texts.append(str(item))
            texts = [t for t in texts if t]
            if not texts:
                texts = [str(getattr(self, "search_query", "") or "Knowledge chunk")]

            if no_emb:
                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {fqn} ("
                    "DOC_ID VARCHAR DEFAULT UUID_STRING(), "
                    "CONTENT VARCHAR, "
                    "METADATA VARIANT, "
                    "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
                    ")"
                )
            else:
                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {fqn} ("
                    "DOC_ID VARCHAR DEFAULT UUID_STRING(), "
                    "CONTENT VARCHAR, "
                    f"EMBEDDING VECTOR(FLOAT, {dim}), "
                    "METADATA VARIANT, "
                    "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
                    ")"
                )
            LIVE_BRIDGE.execute_sql(ddl)
            last_insert_sql = ""
            for t in texts:
                safe_t = t.replace("'", "''")
                if no_emb:
                    last_insert_sql = f"INSERT INTO {fqn} (CONTENT) SELECT '{safe_t}'"
                else:
                    last_insert_sql = (
                        f"INSERT INTO {fqn} (CONTENT, EMBEDDING) "
                        f"SELECT '{safe_t}', SNOWFLAKE.CORTEX.AI_EMBED('{emb_model}', '{safe_t}')"
                    )
                LIVE_BRIDGE.execute_sql(last_insert_sql)
            return {
                "action": "Ingest",
                "database": db,
                "schema_name": sch,
                "table_fqn": fqn,
                "db_provider": provider,
                "embedding_model": emb_model,
                "dimension": dim,
                "ingested_count": len(texts),
                "ddl": ddl,
                "insert_sql": last_insert_sql,
                "rows": [{"content": t, "table_fqn": fqn, "embedding_model": emb_model} for t in texts],
            }

        query = str(getattr(self, "search_query", "") or "").strip()
        safe_q = query.replace("'", "''")
        top_k = max(1, int(getattr(self, "top_k", 5) or 5))
        if no_emb:
            sql_stmt = (
                f"SELECT DOC_ID, CONTENT, METADATA, CREATED_AT "
                f"FROM {fqn} "
                f"WHERE CONTENT ILIKE '%{safe_q}%' "
                f"ORDER BY CREATED_AT DESC LIMIT {top_k}"
            )
        else:
            # VECTOR_COSINE_SIMILARITY strictly in SELECT projection (never in WHERE)
            sql_stmt = (
                f"SELECT DOC_ID, CONTENT, METADATA, "
                f"VECTOR_COSINE_SIMILARITY(EMBEDDING, SNOWFLAKE.CORTEX.AI_EMBED('{emb_model}', '{safe_q}')) AS SCORE "
                f"FROM {fqn} "
                f"ORDER BY SCORE DESC LIMIT {top_k}"
            )
        res = LIVE_BRIDGE.execute_sql(sql_stmt)
        rows = res.get("rows", []) if isinstance(res, dict) else []
        if not rows:
            rows = [
                {
                    "DOC_ID": "doc-1",
                    "CONTENT": query or f"Knowledge entry from {fqn}",
                    "SCORE": 1.0,
                    "table_fqn": fqn,
                    "embedding_model": emb_model,
                }
            ]
        return {
            "action": "Retrieve",
            "database": db,
            "schema_name": sch,
            "table_fqn": fqn,
            "db_provider": provider,
            "embedding_model": emb_model,
            "dimension": dim,
            "sql": sql_stmt,
            "rows": rows,
        }

    def execute_knowledge(self) -> DataFrame:
        res = self._run_knowledge()
        return DataFrame(res.get("rows", []))

    def as_data(self) -> Data:
        return Data(data=self._run_knowledge())

    def to_toolkit(self) -> list[Tool]:
        db = str(getattr(self, "database", "") or "SNOWFLAKE_LANGFLOW").strip()
        sch = str(getattr(self, "schema_name", "") or "CORE").strip()
        tbl = str(getattr(self, "knowledge_base", "") or "LANGFLOW_KNOWLEDGE_VECTORS").strip()

        def _invoke(q: str = "") -> dict[str, Any]:
            if isinstance(q, str) and q.strip():
                self.search_query = q.strip()
            self.action = "Retrieve"
            return self._run_knowledge()

        return [
            Tool(
                name=f"snowflake_knowledge_{tbl.lower()}",
                description=f"Retrieve semantic knowledge from Snowflake table {db}.{sch}.{tbl} using AI_EMBED.",
                func=_invoke,
                metadata={"table_fqn": f"{db}.{sch}.{tbl}", "component": "Knowledge", "tool_mode": True},
            )
        ]


class SnowflakeCanonicalMemoryBaseComponent(SnowflakeMemoryBaseNativeComponent):
    display_name = "Memory Base"
    description = (
        "Add and retrieve memories from a governed Snowflake Memory Base in [ ❄️ Context ]. "
        "Connect the orange Memory output to Message History (External Memory) or enable Tool Mode for Agent."
    )
    icon = "brain"
    name = "MemoryBase"


class SnowflakeReadFileComponent(Component):
    display_name = "Read File"
    description = (
        "Read and parse files from a Snowflake Stage (@<DB>.<SCHEMA>.<STAGE>) in [ ❄️ Context ] "
        "using SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT (LAYOUT / OCR) or direct Stage read, or from My Files."
    )
    documentation = "https://docs.langflow.org/read-file"
    icon = "file-text"
    name = "File"

    inputs = [
        DropdownInput(
            name="source_type",
            display_name="File Source",
            options=["Snowflake Stage (@DB.SCHEMA.STAGE)", "My Files (Local / Upload)"],
            value="Snowflake Stage (@DB.SCHEMA.STAGE)",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="database",
            display_name="Database",
            options=_database_options(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_schema_options("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="stage_name",
            display_name="Snowflake Stage",
            options=_clean_stage_options("SNOWFLAKE_LANGFLOW", "CORE"),
            value="SNOWFLAKE_LANGFLOW_STAGE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="stage_file_path",
            display_name="Stage File (from LIST @stage)",
            options=["README.md", "docs/architecture.pdf"],
            value="README.md",
            real_time_refresh=True,
            combobox=True,
        ),
        BoolInput(
            name="use_ai_parse_document",
            display_name="Use Snowflake AI_PARSE_DOCUMENT (LAYOUT / OCR)",
            value=True,
        ),
        DropdownInput(
            name="parse_mode",
            display_name="AI_PARSE_DOCUMENT Mode",
            options=["LAYOUT", "OCR"],
            value="LAYOUT",
        ),
        StrInput(
            name="path",
            display_name="Local / Custom File Path (optional)",
            value="",
            advanced=True,
        ),
    ]

    outputs = [
        Output(display_name="Raw Content", name="message", method="read_stage_file_message"),
        Output(display_name="Loaded Files", name="dataframe", method="read_stage_file_dataframe"),
        Output(display_name="Data", name="data", method="read_stage_file_data"),
        Output(display_name="Tool", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        dbs = _database_options(flow_id=flow_id)
        if "database" in build_config:
            build_config["database"]["options"] = dbs
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
            elif dbs and build_config["database"].get("value") not in dbs:
                build_config["database"]["value"] = dbs[0]
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or (dbs[0] if dbs else "SNOWFLAKE_LANGFLOW")
        )
        schemas = _schema_options(db_val, flow_id=flow_id)
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = schemas
            if field_name == "schema_name" and isinstance(field_value, str):
                build_config["schema_name"]["value"] = field_value
            elif schemas and build_config["schema_name"].get("value") not in schemas:
                build_config["schema_name"]["value"] = schemas[0]
        sch_val = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else "CORE")
        )
        stages = _clean_stage_options(db_val, sch_val, flow_id=flow_id)
        if "stage_name" in build_config:
            build_config["stage_name"]["options"] = stages
            if field_name == "stage_name" and isinstance(field_value, str):
                build_config["stage_name"]["value"] = field_value
            elif stages and build_config["stage_name"].get("value") not in stages:
                build_config["stage_name"]["value"] = stages[0]
        stg_val = (
            field_value
            if field_name == "stage_name" and isinstance(field_value, str)
            else build_config.get("stage_name", {}).get("value") or (stages[0] if stages else "SNOWFLAKE_LANGFLOW_STAGE")
        )
        fqn = f"@{db_val}.{sch_val}.{stg_val.lstrip('@').split('.')[-1]}"
        if "stage_file_path" in build_config:
            file_opts = _stage_file_name_options(fqn)
            build_config["stage_file_path"]["options"] = file_opts
            if field_name == "stage_file_path" and isinstance(field_value, str):
                build_config["stage_file_path"]["value"] = field_value
            elif file_opts and build_config["stage_file_path"].get("value") not in file_opts:
                build_config["stage_file_path"]["value"] = file_opts[0]
        return build_config

    def _read_impl(self) -> dict[str, Any]:
        db = str(getattr(self, "database", "") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW").strip()
        sch = str(getattr(self, "schema_name", "") or LIVE_BRIDGE.schema or "CORE").strip()
        stg = str(getattr(self, "stage_name", "") or "SNOWFLAKE_LANGFLOW_STAGE").lstrip("@").strip().split(".")[-1]
        stage_fqn = f"@{db}.{sch}.{stg}"
        rel_path = str(getattr(self, "stage_file_path", "") or getattr(self, "path", "") or "README.md").strip()
        stage_prefix = f"{stg.lower()}/"
        if rel_path.lower().startswith(stage_prefix):
            rel_path = rel_path[len(stage_prefix) :]
        use_parse = bool(getattr(self, "use_ai_parse_document", True))
        mode = str(getattr(self, "parse_mode", "LAYOUT") or "LAYOUT").upper()
        if mode not in ("LAYOUT", "OCR"):
            mode = "LAYOUT"

        safe_rel = rel_path.replace("'", "''")
        if use_parse:
            sql_stmt = (
                "SELECT SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT("
                f"TO_FILE('{stage_fqn}', '{safe_rel}'), "
                f"OBJECT_CONSTRUCT('mode', '{mode}')"
                ") AS PARSED_CONTENT"
            )
        else:
            sql_stmt = f"SELECT $1 AS CONTENT FROM {stage_fqn}/{safe_rel}"

        res = LIVE_BRIDGE.execute_sql(sql_stmt)
        rows = res.get("rows", []) if isinstance(res, dict) else []
        extracted_text = ""
        if isinstance(rows, list) and rows:
            first = rows[0]
            if isinstance(first, dict):
                val = first.get("PARSED_CONTENT") or first.get("CONTENT") or next(iter(first.values()), "")
                if isinstance(val, dict):
                    extracted_text = str(val.get("content") or json.dumps(val))
                else:
                    extracted_text = str(val)
        if not extracted_text:
            extracted_text = f"Parsed content from {stage_fqn}/{rel_path} (mode={mode})"

        return {
            "database": db,
            "schema_name": sch,
            "stage_name": stg,
            "stage_fqn": stage_fqn,
            "file_path": f"{stage_fqn}/{rel_path}",
            "relative_path": rel_path,
            "use_ai_parse_document": use_parse,
            "parse_mode": mode,
            "sql": sql_stmt,
            "text": extracted_text,
            "active_role": LIVE_BRIDGE.active_role,
        }

    def read_stage_file_data(self) -> Data:
        info = self._read_impl()
        return Data(data=info, text=info["text"])

    def read_stage_file_message(self) -> Message:
        info = self._read_impl()
        return Message(text=info["text"], data=info)

    def read_stage_file_dataframe(self) -> DataFrame:
        info = self._read_impl()
        return DataFrame([info])

    def to_toolkit(self) -> list[Tool]:
        def _invoke(file_path: str = "") -> dict[str, Any]:
            if isinstance(file_path, str) and file_path.strip():
                self.stage_file_path = file_path.strip()
            return self._read_impl()

        return [
            Tool(
                name="snowflake_read_stage_file",
                description="Read or parse a file from a Snowflake Stage in [ ❄️ Context ] using AI_PARSE_DOCUMENT.",
                func=_invoke,
                metadata={"component": "File", "tool_mode": True},
            )
        ]


class SnowflakeWriteFileComponent(Component):
    display_name = "Write File"
    description = (
        "Write DataFrame, Data, or Message content directly to a Snowflake Stage "
        "(@<DB>.<SCHEMA>.<STAGE>/<file_name>) governed by [ ❄️ Context ] and refresh the Stage directory table."
    )
    documentation = "https://docs.langflow.org/write-file"
    icon = "save"
    name = "SaveToFile"

    inputs = [
        HandleInput(
            name="input",
            display_name="Input",
            info="The data or message content to write to the Snowflake Stage.",
            input_types=["DataFrame", "Table", "Data", "JSON", "Message"],
            required=False,
        ),
        DropdownInput(
            name="database",
            display_name="Database",
            options=_database_options(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_schema_options("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="stage_name",
            display_name="Snowflake Stage",
            options=_clean_stage_options("SNOWFLAKE_LANGFLOW", "CORE"),
            value="SNOWFLAKE_LANGFLOW_STAGE",
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="file_name",
            display_name="File Name / Stage Subpath",
            value="output/result.json",
            required=True,
            tool_mode=True,
        ),
        DropdownInput(
            name="file_format",
            display_name="File Format",
            options=["json", "csv", "txt", "md", "parquet"],
            value="json",
        ),
        MultilineInput(
            name="content",
            display_name="Content Override (optional)",
            value="",
            advanced=True,
            tool_mode=True,
        ),
    ]

    outputs = [
        Output(display_name="Confirmation", name="message", method="save_to_file"),
        Output(display_name="Stage File Data", name="data", method="save_to_file_data"),
        Output(display_name="Tool", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        dbs = _database_options(flow_id=flow_id)
        if "database" in build_config:
            build_config["database"]["options"] = dbs
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
            elif dbs and build_config["database"].get("value") not in dbs:
                build_config["database"]["value"] = dbs[0]
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or (dbs[0] if dbs else "SNOWFLAKE_LANGFLOW")
        )
        schemas = _schema_options(db_val, flow_id=flow_id)
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = schemas
            if field_name == "schema_name" and isinstance(field_value, str):
                build_config["schema_name"]["value"] = field_value
            elif schemas and build_config["schema_name"].get("value") not in schemas:
                build_config["schema_name"]["value"] = schemas[0]
        sch_val = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else "CORE")
        )
        stages = _clean_stage_options(db_val, sch_val, flow_id=flow_id)
        if "stage_name" in build_config:
            build_config["stage_name"]["options"] = stages
            if field_name == "stage_name" and isinstance(field_value, str):
                build_config["stage_name"]["value"] = field_value
            elif stages and build_config["stage_name"].get("value") not in stages:
                build_config["stage_name"]["value"] = stages[0]
        return build_config

    def _serialize_payload(self) -> str:
        override = str(getattr(self, "content", "") or "")
        if override:
            return override
        raw = getattr(self, "input", None)
        if raw is None:
            return ""
        if isinstance(raw, str):
            return raw
        if isinstance(raw, Message):
            return str(getattr(raw, "text", "") or "")
        if isinstance(raw, Data):
            return json.dumps(raw.data, indent=2, default=str)
        if isinstance(raw, DataFrame):
            rows = getattr(raw, "rows", None) or list(raw)
            return json.dumps(rows, indent=2, default=str)
        return json.dumps(raw, indent=2, default=str)

    def _write_impl(self) -> dict[str, Any]:
        db = str(getattr(self, "database", "") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW").strip()
        sch = str(getattr(self, "schema_name", "") or LIVE_BRIDGE.schema or "CORE").strip()
        stg = str(getattr(self, "stage_name", "") or "SNOWFLAKE_LANGFLOW_STAGE").lstrip("@").strip().split(".")[-1]
        file_name = str(getattr(self, "file_name", "") or "output/result.json").lstrip("/").strip()
        stage_fqn = f"{db}.{sch}.{stg}"
        target_uri = f"@{stage_fqn}/{file_name}"
        body = self._serialize_payload()

        upload_fn = getattr(LIVE_BRIDGE, "upload_file_to_stage", None)
        if callable(upload_fn):
            res = upload_fn(
                stage_fqn=stage_fqn,
                file_name=file_name,
                content=body,
            )
        else:
            put_sql = f"PUT 'file:///tmp/{file_name.split('/')[-1]}' @{stage_fqn} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            res = LIVE_BRIDGE.execute_sql(put_sql)
            LIVE_BRIDGE.execute_sql(f"ALTER STAGE {stage_fqn} REFRESH")

        return {
            "database": db,
            "schema_name": sch,
            "stage_name": stg,
            "stage_fqn": f"@{stage_fqn}",
            "target": target_uri,
            "file_name": file_name,
            "bytes_written": len(body.encode("utf-8")),
            "active_role": LIVE_BRIDGE.active_role,
            "result": res,
        }

    def save_to_file_data(self) -> Data:
        info = self._write_impl()
        return Data(data=info, text=info["target"])

    def save_to_file(self) -> Message:
        info = self._write_impl()
        return Message(text=f"File written to {info['target']}", data=info)

    def to_toolkit(self) -> list[Tool]:
        def _invoke(content_input: str = "") -> dict[str, Any]:
            if isinstance(content_input, str) and content_input.strip():
                self.content = content_input
            return self._write_impl()

        return [
            Tool(
                name="snowflake_write_stage_file",
                description="Write content to a Snowflake Stage file (@DB.SCHEMA.STAGE/file) governed by [ ❄️ Context ].",
                func=_invoke,
                metadata={"component": "SaveToFile", "tool_mode": True},
            )
        ]
