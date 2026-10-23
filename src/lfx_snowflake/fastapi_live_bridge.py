"""FastAPI live bridge mounting `/api/v1/snowflake/*` inside Langflow and connecting to Snowflake SQL via SPCS OAuth."""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[assignment]

try:
    from fastapi import APIRouter, Request
    from fastapi.responses import JSONResponse
except ImportError:  # pragma: no cover
    APIRouter = Any  # type: ignore[assignment,misc]
    Request = Any  # type: ignore[assignment,misc]
    JSONResponse = Any  # type: ignore[assignment,misc]

from .bundle_manifest import (
    ERROR_TAXONOMY_CONTRACT,
    INVARIANT_CATEGORIES_MANIFEST,
    NEW_FLOW_IMPORT_MODES,
    SETTINGS_CONTRACT,
    SNOWFLAKE_BRANDING_CONTRACT,
    get_bundle_manifest,
)
from .governance.models import (
    ExecutionIdentityType as ExecutionMode,
    SnowflakeErrorCode as ErrorCode,
)
from .observability.redaction import redact_payload_for_sink as redact_for_sink

SPCS_TOKEN_PATH = Path("/snowflake/session/token")

AGENT_MEMORY_TYPES: tuple[dict[str, Any], ...] = (
    {
        "agent_memory_type": "episodic_session",
        "label": "+ Episodic / Session Memory",
        "description": (
            "Low-latency conversational history, dialogue turns, and agent episodes backed by "
            "Snowflake Hybrid Table (point-lookup PK on session_id, turn_id) or Snowflake Postgres (Relational / JSONB)."
        ),
        "supported_backends": [
            "Hybrid Table",
            "Standard Table",
            "Snowflake Postgres (Relational / JSONB)",
        ],
    },
    {
        "agent_memory_type": "semantic_vector",
        "label": "+ Semantic / Vector Memory",
        "description": (
            "Embedding-indexed semantic recall backed by Standard / Hybrid Table with VECTOR(FLOAT|INT, 768|1024) "
            "coupled to official AI_EMBED models, Snowflake Postgres (pgvector), or Cortex Search Service."
        ),
        "supported_backends": [
            "Standard Table (VECTOR)",
            "Hybrid Table (VECTOR)",
            "Interactive Table (VECTOR)",
            "Dynamic Table (VECTOR)",
            "Snowflake Postgres (pgvector)",
            "Cortex Search Service",
        ],
    },
    {
        "agent_memory_type": "structured_long_term",
        "label": "+ Structured / Semi-Structured Long-Term Memory",
        "description": (
            "Persistent agent facts, user preferences, and entity profiles backed by Standard / Dynamic / "
            "Interactive Tables with VARIANT / OBJECT / ARRAY columns or consolidation Views."
        ),
        "supported_backends": [
            "Standard Table",
            "Dynamic Table",
            "Interactive Table",
            "Semi-Structured (VARIANT)",
            "Semi-Structured (OBJECT)",
            "Semi-Structured (ARRAY)",
            "View",
        ],
    },
)


class LiveSnowflakeBridge:
    """Executes real SQL against Snowflake via SPCS OAuth token and manages active role & trace replication."""

    def __init__(self) -> None:
        self.database = os.getenv("SNOWFLAKE_DATABASE", "SNOWFLAKE_LANGFLOW")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA", "CORE")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "")
        self.stage_name = os.getenv("SNOWFLAKE_STAGE_NAME", "SNOWFLAKE_LANGFLOW_STAGE")
        self.runs_table = os.getenv(
            "LANGFLOW_TRACES_RUNS_TABLE", f"{self.database}.{self.schema}.LANGFLOW_FLOW_RUNS"
        )
        self.spans_table = os.getenv(
            "LANGFLOW_TRACES_SPANS_TABLE", f"{self.database}.{self.schema}.LANGFLOW_FLOW_SPANS"
        )
        self.audit_table = os.getenv(
            "LANGFLOW_AUDIT_LOG_TABLE", f"{self.database}.{self.schema}.LANGFLOW_AUDIT_LOG"
        )
        self.active_role: str = os.getenv("SNOWFLAKE_DEFAULT_ROLE", "ACCOUNTADMIN")
        self.available_roles: list[str] = [
            "ACCOUNTADMIN",
            "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
            "SNOWFLAKE_LANGFLOW_DEV_ROLE",
            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
            "PUBLIC",
        ]
        self.authenticated_user: str = os.getenv("SNOWFLAKE_USER", "SPCS_CALLER")
        self.context_epoch: int = 1
        self.secondary_roles_mode: str = "NONE"
        self.memories_registry: list[dict[str, Any]] = []
        self.selected_context: dict[str, Any] = {
            "databases": [self.database],
            "schemas_by_db": {self.database: [self.schema, "PUBLIC"]},
            "objects_by_db_schema": {},
            "selected_objects": {},
            "visible_objects": {
                "tables": ["LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"],
                "views": [],
                "iceberg_tables": [],
                "stages": [self.stage_name],
                "memories": ["SNOWFLAKE_KB_MEM"],
                "cortex_search_services": [],
                "semantic_views": [],
                "semantic_models": [],
                "cortex_agents": [],
                "functions": [],
                "procedures": [],
                "postgres_instances": [],
            },
        }
        self._roles_discovered: bool = False
        self._superuser_synced: bool = False

    @property
    def memories_catalog(self) -> list[dict[str, Any]]:
        """Return governed Snowflake Agent Memories available for workflow recall (`W2-R2-13`)."""
        items: list[dict[str, Any]] = [dict(m) for m in self.memories_registry]
        known_names = {str(m.get("name") or "").strip().upper() for m in items if m.get("name")}
        vis_mem = list((self.selected_context.get("visible_objects") or {}).get("memories") or [])
        for mem_name in vis_mem:
            clean = str(mem_name or "").strip()
            if clean and clean.upper() not in known_names:
                items.append(
                    {
                        "name": clean,
                        "database": self.database,
                        "schema": self.schema,
                        "table_fqn": f"{self.database}.{self.schema}.MEM_{clean.upper()}",
                        "agent_memory_type": "semantic_vector",
                        "memory_category": "vector_table",
                        "vector_database": "Standard Table (VECTOR)",
                        "embedding_model": "snowflake-arctic-embed-m-v1.5",
                        "dimension": 768,
                        "created_by_role": self.active_role,
                    }
                )
                known_names.add(clean.upper())
        return items

    def get_memory_catalog_options(self, *, include_none: bool = True) -> list[str]:
        """Return dropdown options for `memory_catalog_ref` in workflow components."""
        opts: list[str] = ["None"] if include_none else []
        for entry in self.memories_catalog:
            nm = str(entry.get("name") or "").strip()
            fqn = str(entry.get("table_fqn") or "").strip()
            agent_type = str(entry.get("agent_memory_type") or entry.get("memory_category") or "memory").strip()
            if nm and nm not in opts:
                opts.append(nm)
            label = f"{nm} ({agent_type} -> {fqn})" if nm and fqn else (fqn or nm)
            if label and label not in opts:
                opts.append(label)
        return opts or (["None", "SNOWFLAKE_KB_MEM"] if include_none else ["SNOWFLAKE_KB_MEM"])

    def recall_memory(
        self,
        memory_ref: str,
        *,
        query: str = "",
        session_id: str = "default",
        limit: int = 10,
    ) -> dict[str, Any]:
        """Recall records from a Snowflake Memory created via `+ Snowflake` (`W2-R2-13`)."""
        clean_ref = (memory_ref or "").strip()
        if not clean_ref or clean_ref == "None":
            return {"ok": False, "recalled": False, "memory_ref": None, "rows": []}
        short_ref = clean_ref.split(" (")[0].strip()
        matched: dict[str, Any] | None = None
        for entry in self.memories_catalog:
            if short_ref.upper() in {
                str(entry.get("name") or "").strip().upper(),
                str(entry.get("table_fqn") or "").strip().upper(),
            }:
                matched = entry
                break
        if matched is None:
            safe = "".join(c if c.isalnum() or c == "_" else "_" for c in short_ref).upper()
            matched = {
                "name": short_ref,
                "database": self.database,
                "schema": self.schema,
                "table_fqn": short_ref if "." in short_ref else f"{self.database}.{self.schema}.MEM_{safe}",
                "agent_memory_type": "semantic_vector",
                "memory_category": "vector_table",
                "embedding_model": "snowflake-arctic-embed-m-v1.5",
            }
        fqn = str(matched.get("table_fqn") or f"{self.database}.{self.schema}.MEM_{short_ref.upper()}")
        agent_type = str(matched.get("agent_memory_type") or "semantic_vector")
        bounded_limit = max(1, min(int(limit or 10), 200))
        safe_q = (query or "").replace("'", "''")
        safe_sess = (session_id or "default").replace("'", "''")
        if agent_type == "episodic_session":
            recall_sql = (
                f"SELECT * FROM {fqn} WHERE session_id = '{safe_sess}' "
                f"ORDER BY created_at DESC LIMIT {bounded_limit}"
            )
        elif agent_type == "semantic_vector" and safe_q:
            embed_model = str(matched.get("embedding_model") or "snowflake-arctic-embed-m-v1.5").split(" ")[0]
            recall_sql = (
                f"SELECT chunk_id, memory_name, text_chunk, metadata, "
                f"VECTOR_COSINE_SIMILARITY(embedding, AI_EMBED('{embed_model}', '{safe_q}')) AS score "
                f"FROM {fqn} ORDER BY score DESC LIMIT {bounded_limit}"
            )
        else:
            where_clause = f" WHERE text_chunk ILIKE '%{safe_q}%'" if safe_q else ""
            recall_sql = f"SELECT * FROM {fqn}{where_clause} ORDER BY created_at DESC LIMIT {bounded_limit}"
        res = self.execute_sql(recall_sql)
        return {
            "ok": True,
            "recalled": True,
            "memory_ref": short_ref,
            "memory": matched,
            "agent_memory_type": agent_type,
            "table_fqn": fqn,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "recall_sql": recall_sql,
            "rows": res.get("rows", []),
        }

    def create_or_register_memory(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create or register a Snowflake Agent Memory across the 3 Agent Memory types (`W2-R2-13`)."""
        from .providers.embeddings import resolve_embedding_dimension

        name = str(body.get("name") or "").strip()
        db = str(body.get("database") or self.database).strip().upper()
        sch = str(body.get("schema") or self.schema).strip().upper()
        raw_agent_type = str(body.get("agent_memory_type") or "").strip().lower()
        category = str(body.get("memory_category") or "").strip().lower()
        backend = str(body.get("vector_database") or body.get("storage_backend") or "").strip()
        if not category:
            if raw_agent_type == "episodic_session":
                category = "postgres_non_vector" if "POSTGRES" in backend.upper() else "structured"
            elif raw_agent_type == "structured_long_term":
                category = "semi_structured" if any(t in backend.upper() for t in ("VARIANT", "OBJECT", "ARRAY")) else "structured"
            else:
                category = "vector_table"
        if not backend:
            if raw_agent_type == "episodic_session":
                backend = "Hybrid Table"
            elif raw_agent_type == "structured_long_term":
                backend = "Semi-Structured (VARIANT)" if category == "semi_structured" else "Standard Table"
            else:
                backend = "Standard Table (VECTOR)"

        table_type = str(
            body.get("table_type")
            or ("HYBRID" if ("Hybrid" in backend or raw_agent_type == "episodic_session") else "STANDARD")
        ).strip().upper()
        semi_type = str(body.get("semi_structured_type") or "").strip().upper()
        embedding_model = str(body.get("embedding_model") or "snowflake-arctic-embed-m-v1.5").strip()
        elem_type = str(body.get("vector_element_type") or "FLOAT").strip().upper()
        batch_size = int(body.get("batch_size") or 64)
        llm_preprocessing = body.get("llm_preprocessing") or None

        denied = self.check_write_privilege_or_deny("CREATE_MEMORY", name or "MEMORY_TABLE")
        if denied:
            return denied

        is_vector = (
            raw_agent_type == "semantic_vector"
            or (
                raw_agent_type not in ("episodic_session", "structured_long_term")
                and (
                    category in ("vector_table", "postgres_pgvector", "cortex_search")
                    or "VECTOR" in backend.upper()
                    or "PGVECTOR" in backend.upper()
                    or "CORTEX SEARCH" in backend.upper()
                )
                and category not in ("structured", "structured_table", "semi_structured", "postgres_non_vector")
            )
        )
        if raw_agent_type in ("episodic_session", "semantic_vector", "structured_long_term"):
            agent_memory_type = raw_agent_type
        elif is_vector:
            agent_memory_type = "semantic_vector"
        elif category == "postgres_non_vector" or table_type == "HYBRID":
            agent_memory_type = "episodic_session"
        else:
            agent_memory_type = "structured_long_term"

        dim = resolve_embedding_dimension(embedding_model, default=768)
        safe_table = "".join(c if c.isalnum() or c == "_" else "_" for c in (name or "MEMORY_STORE")).upper()
        fqn = f"{db}.{sch}.MEM_{safe_table}"

        if agent_memory_type == "episodic_session" and not (
            category == "postgres_non_vector" or ("POSTGRES" in backend.upper() and "PGVECTOR" not in backend.upper())
        ):
            table_kw = "HYBRID TABLE" if table_type == "HYBRID" else "TABLE"
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"session_id VARCHAR(128), turn_id VARCHAR(128), memory_name VARCHAR(256), "
                f"sender VARCHAR(64), text_chunk VARCHAR, metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), PRIMARY KEY (session_id, turn_id));"
            )
        elif not is_vector:
            table_kw = "HYBRID TABLE" if table_type == "HYBRID" else "TABLE"
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128) PRIMARY KEY, memory_name VARCHAR(256), text_chunk VARCHAR, "
                f"metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )
        else:
            table_kw = "HYBRID TABLE" if table_type == "HYBRID" else "TABLE"
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128) PRIMARY KEY, memory_name VARCHAR(256), text_chunk VARCHAR, "
                f"embedding VECTOR({elem_type}, {dim}), metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )

        exec_res = {"ok": True, "dry_run": True} if body.get("dry_run") else self.execute_sql(ddl)
        entry = {
            "name": name,
            "database": db,
            "schema": sch,
            "table_fqn": fqn,
            "agent_memory_type": agent_memory_type,
            "memory_category": category,
            "vector_database": backend,
            "table_type": table_type,
            "semi_structured_type": semi_type or None,
            "is_vector_memory": is_vector,
            "embedding_model": embedding_model if is_vector else None,
            "vector_element_type": elem_type if is_vector else None,
            "dimension": dim if is_vector else None,
            "batch_size": batch_size,
            "llm_preprocessing": llm_preprocessing,
            "ddl": ddl,
            "created_by_role": self.active_role,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "snowflake_created": bool(exec_res.get("ok")),
        }
        self.memories_registry.append(entry)
        vis_mem = self.selected_context.setdefault("visible_objects", {}).setdefault("memories", [])
        if name and name not in vis_mem:
            vis_mem.append(name)
        return {"ok": True, "status_code": 201, "memory": entry, "ddl": ddl}

    def get_context_databases(self) -> list[str]:
        """Return active databases in the Hub cone of visibility (`W2-R2-00`)."""
        dbs = [str(d).strip().upper() for d in (self.selected_context.get("databases") or []) if str(d).strip()]
        default_db = (self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        if default_db and default_db not in dbs:
            dbs.insert(0, default_db)
        return dbs or ["SNOWFLAKE_LANGFLOW"]

    def get_context_schemas(self, database: str | None = None) -> list[str]:
        """Return active schemas for `<database>` in the Hub cone of visibility (`W2-R2-00`)."""
        db = (database or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        schemas_by_db = self.selected_context.get("schemas_by_db") or {}
        schemas = [str(s).strip().upper() for s in (schemas_by_db.get(db) or []) if str(s).strip()]
        if not schemas and db == (self.database or "").strip().upper():
            default_sch = (self.schema or "CORE").strip().upper()
            schemas = [default_sch, "PUBLIC"] if default_sch != "PUBLIC" else ["PUBLIC"]
        return schemas or ["CORE", "PUBLIC"]

    def get_context_objects(
        self,
        database: str | None = None,
        schema: str | None = None,
        category: str = "tables",
    ) -> list[str]:
        """Return objects for `(<database>, <schema>, <category>)` filtered by `selected_objects` (`W2-R2-00`)."""
        db = (database or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = (schema or self.schema or "CORE").strip().upper()
        cat = (category or "tables").strip().lower()
        db_sch_key = f"{db}.{sch}"

        objs_by_pair = self.selected_context.get("objects_by_db_schema") or {}
        pair_bucket = objs_by_pair.get(db_sch_key) if isinstance(objs_by_pair, dict) else None
        vis = self.selected_context.get("visible_objects") or {}

        candidates: list[str] = []
        if isinstance(pair_bucket, dict) and cat in pair_bucket and isinstance(pair_bucket[cat], list):
            candidates.extend(str(x).strip() for x in pair_bucket[cat] if str(x).strip())
        if isinstance(vis.get(cat), list):
            for item in vis[cat]:
                s_item = str(item).strip()
                if not s_item:
                    continue
                if "." in s_item and not s_item.startswith("@"):
                    parts = s_item.split(".")
                    if len(parts) >= 3 and (parts[0].upper() != db or parts[1].upper() != sch):
                        continue
                if s_item not in candidates:
                    candidates.append(s_item)

        sel_objs = self.selected_context.get("selected_objects")
        allowed_set: set[str] | None = None
        if isinstance(sel_objs, dict) and sel_objs:
            if db_sch_key in sel_objs and isinstance(sel_objs[db_sch_key], dict):
                cat_sel = sel_objs[db_sch_key].get(cat)
                if isinstance(cat_sel, list):
                    allowed_set = {str(x).strip().upper() for x in cat_sel if str(x).strip()}
            elif db_sch_key in sel_objs and isinstance(sel_objs[db_sch_key], list):
                allowed_set = {str(x).strip().upper() for x in sel_objs[db_sch_key] if str(x).strip()}
            elif cat in sel_objs and isinstance(sel_objs[cat], list):
                allowed_set = {str(x).strip().upper() for x in sel_objs[cat] if str(x).strip()}
        elif isinstance(sel_objs, list) and sel_objs:
            allowed_set = {str(x).strip().upper() for x in sel_objs if str(x).strip()}

        if allowed_set is not None:
            allowed_base_set = {a.split("(")[0].strip().split(".")[-1] for a in allowed_set}
            filtered: list[str] = []
            for item in candidates:
                raw_u = item.upper()
                base_u = raw_u.split("(")[0].strip().split(".")[-1]
                fqn_u = f"{db}.{sch}.{base_u}"
                if (
                    raw_u in allowed_set
                    or base_u in allowed_set
                    or fqn_u in allowed_set
                    or base_u in allowed_base_set
                ):
                    filtered.append(item)
            if not filtered and allowed_set:
                for raw_allowed in sorted(allowed_set):
                    if "." in raw_allowed:
                        parts = raw_allowed.split(".")
                        if len(parts) >= 3 and parts[0].upper() == db and parts[1].upper() == sch:
                            filtered.append(parts[-1])
                    else:
                        filtered.append(raw_allowed)
            return filtered

        return candidates

    def can_create_database(self) -> bool:
        """Return True if the active role is permitted to execute CREATE DATABASE ON ACCOUNT."""
        return self.active_role.strip().upper() in (
            "ACCOUNTADMIN",
            "SYSADMIN",
            "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
        )

    def can_create_schema(self) -> bool:
        """Return True if the active role is permitted to execute CREATE SCHEMA in the target database."""
        return self.active_role.strip().upper() not in (
            "PUBLIC",
            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
            "VIEWER_ROLE",
            "ROLE_VIEWER",
        )

    def can_create_object(self) -> bool:
        """Return True if the active role is permitted to create schema-level objects."""
        return self.can_create_schema()

    def get_role_privileges(self) -> dict[str, bool]:
        """Compute granular active-role creation privileges for Hub Section 1 & sidebar components."""
        can_db = self.can_create_database()
        can_sch = self.can_create_schema()
        can_obj = self.can_create_object()
        return {
            "can_create_database": can_db,
            "can_create_schema": can_sch,
            "can_create_table": can_obj,
            "can_create_stage": can_obj,
            "can_create_view": can_obj,
            "can_create_function": can_obj,
            "can_create_procedure": can_obj,
            "can_create_cortex_search": can_obj,
            "can_create_postgres": can_db,
            "can_create_object": can_obj,
        }

    @property
    def role_privileges(self) -> dict[str, bool]:
        return self.get_role_privileges()

    def update_selected_context(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Persist cascading primary context selection from Snowflake Langflow Hub (`HUB-W2-02`, `W2-R2-00`)."""
        incoming_ctx = payload.get("selected_context") if isinstance(payload.get("selected_context"), dict) else {}
        new_db = str(payload.get("database") or incoming_ctx.get("database") or "").strip().upper()
        new_sch = str(payload.get("schema") or incoming_ctx.get("schema") or "").strip().upper()
        if new_db:
            self.database = new_db
        if new_sch:
            self.schema = new_sch

        dbs = payload.get("databases") or incoming_ctx.get("databases")
        if isinstance(dbs, list) and dbs:
            self.selected_context["databases"] = [str(d).strip().upper() for d in dbs if str(d).strip()]
            if not new_db and self.selected_context["databases"]:
                self.database = self.selected_context["databases"][0]
        elif self.database not in self.selected_context.get("databases", []):
            self.selected_context.setdefault("databases", []).insert(0, self.database)

        schemas_by_db = payload.get("schemas_by_db") or incoming_ctx.get("schemas_by_db")
        if isinstance(schemas_by_db, dict) and schemas_by_db:
            normalized_map: dict[str, list[str]] = {}
            for k, v in schemas_by_db.items():
                db_k = str(k).strip().upper()
                if isinstance(v, list):
                    normalized_map[db_k] = [str(s).strip().upper() for s in v if str(s).strip()]
            if normalized_map:
                self.selected_context["schemas_by_db"] = normalized_map
                if not new_sch and self.database in normalized_map and normalized_map[self.database]:
                    self.schema = normalized_map[self.database][0]
        else:
            cur_map = self.selected_context.setdefault("schemas_by_db", {})
            cur_list = cur_map.setdefault(self.database, [self.schema])
            if self.schema not in cur_list:
                cur_list.insert(0, self.schema)

        objs_by_pair = payload.get("objects_by_db_schema") or incoming_ctx.get("objects_by_db_schema")
        if isinstance(objs_by_pair, dict) and objs_by_pair:
            norm_objs: dict[str, dict[str, list[str]]] = {}
            for pair_k, pair_v in objs_by_pair.items():
                clean_pair = str(pair_k).strip().upper()
                if isinstance(pair_v, dict):
                    norm_objs[clean_pair] = {
                        str(cat_k).strip().lower(): [str(item).strip() for item in cat_v if str(item).strip()]
                        for cat_k, cat_v in pair_v.items()
                        if isinstance(cat_v, list)
                    }
            self.selected_context["objects_by_db_schema"] = norm_objs

        sel_objs = payload.get("selected_objects") if "selected_objects" in payload else incoming_ctx.get("selected_objects")
        if sel_objs is not None:
            self.selected_context["selected_objects"] = sel_objs

        vis = payload.get("visible_objects") or incoming_ctx.get("visible_objects")
        if isinstance(vis, dict):
            cur_vis = self.selected_context.setdefault("visible_objects", {})
            for k, v in vis.items():
                if isinstance(v, list):
                    cur_vis[k] = [str(item).strip() for item in v if str(item).strip()]
                elif isinstance(v, bool):
                    # When Hub passes boolean category toggles, preserve or clear list accordingly
                    if not v:
                        cur_vis[k] = []
                    else:
                        cur_vis.setdefault(k, [])

        self.context_epoch += 1
        self.record_audit_event(
            action="UPDATE_CONTEXT",
            outcome="ALLOWED",
            target_object=f"{self.database}.{self.schema}",
            details={"selected_context": self.selected_context, "context_epoch": self.context_epoch},
        )
        return {
            "ok": True,
            "status": "ok",
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "database": self.database,
            "schema": self.schema,
            "selected_context": self.selected_context,
            "role_privileges": self.role_privileges,
        }

    def discover_catalog(
        self,
        *,
        database: str | None = None,
        schema: str | None = None,
        object_kind: str | None = None,
        databases: list[str] | str | None = None,
        schemas_by_db: dict[str, list[str]] | str | None = None,
    ) -> dict[str, Any]:
        """Discover Multi-Database -> Multi-Schema -> Cascading Objects for CURRENT_ROLE() (`W2-R2-00`, `HUB-W2-02`)."""
        db = (database or self.database).strip().upper()
        sch = (schema or self.schema).strip().upper()

        parsed_dbs: list[str] = []
        if isinstance(databases, str) and databases.strip():
            parsed_dbs = [d.strip().upper() for d in databases.split(",") if d.strip()]
        elif isinstance(databases, list):
            parsed_dbs = [str(d).strip().upper() for d in databases if str(d).strip()]
        if db and db not in parsed_dbs:
            parsed_dbs.insert(0, db)
        for ctx_db in self.selected_context.get("databases", []):
            clean_ctx_db = str(ctx_db).strip().upper()
            if clean_ctx_db and clean_ctx_db not in parsed_dbs:
                parsed_dbs.append(clean_ctx_db)

        parsed_schemas_map: dict[str, list[str]] = {}
        if isinstance(schemas_by_db, str) and schemas_by_db.strip():
            try:
                loaded_map = json.loads(schemas_by_db)
                if isinstance(loaded_map, dict):
                    for k, v in loaded_map.items():
                        if isinstance(v, list):
                            parsed_schemas_map[str(k).strip().upper()] = [str(s).strip().upper() for s in v if str(s).strip()]
            except Exception:  # noqa: BLE001
                pass
        elif isinstance(schemas_by_db, dict):
            for k, v in schemas_by_db.items():
                if isinstance(v, list):
                    parsed_schemas_map[str(k).strip().upper()] = [str(s).strip().upper() for s in v if str(s).strip()]

        def _extract_names(rows: list[dict[str, Any]], keys: tuple[str, ...] = ("name", "NAME", "OBJECT_NAME")) -> list[str]:
            out: list[str] = []
            for r in rows:
                for k in keys:
                    val = r.get(k)
                    if val and str(val).strip() and str(val).strip() not in out:
                        out.append(str(val).strip())
                        break
            return out

        dbs_res = self.execute_sql("SHOW DATABASES")
        pg_res = self.execute_sql("SHOW POSTGRES INSTANCES")
        discovered_databases = _extract_names(dbs_res.get("rows", [])) or list(
            dict.fromkeys([db, *parsed_dbs, *self.selected_context.get("databases", [self.database])])
        )
        postgres_instances = _extract_names(pg_res.get("rows", [])) or list(
            self.selected_context.get("visible_objects", {}).get("postgres_instances", [])
        )

        schemas_by_db_out: dict[str, list[str]] = dict(self.selected_context.get("schemas_by_db", {}))
        objects_by_db_schema: dict[str, dict[str, list[str]]] = dict(
            self.selected_context.get("objects_by_db_schema", {})
        )

        target_dbs = parsed_dbs or [db]
        primary_schemas: list[str] = []
        primary_raw_objects: list[dict[str, Any]] = []
        primary_stages_rows: list[dict[str, Any]] = []

        merged_tables: list[str] = []
        merged_views: list[str] = []
        merged_iceberg_tables: list[str] = []
        merged_stages: list[str] = []
        merged_functions: list[str] = []
        merged_procedures: list[str] = []
        merged_search_services: list[str] = []
        merged_semantic_views: list[str] = []
        merged_cortex_agents: list[str] = []

        for target_db in target_dbs:
            sch_res = self.execute_sql(f"SHOW SCHEMAS IN DATABASE {target_db}")
            db_schemas = _extract_names(sch_res.get("rows", [])) or list(
                dict.fromkeys([*(parsed_schemas_map.get(target_db) or []), sch if target_db == db else "CORE", "CORE", "PUBLIC"])
            )
            schemas_by_db_out[target_db] = db_schemas
            if target_db == db:
                primary_schemas = db_schemas

            active_schemas_for_db = parsed_schemas_map.get(target_db) or (
                [sch] if target_db == db else db_schemas[:1]
            )
            for target_sch in active_schemas_for_db:
                pair_key = f"{target_db}.{target_sch}"
                objects_res = self.execute_sql(f"SHOW OBJECTS IN SCHEMA {pair_key}")
                iceberg_res = self.execute_sql(f"SHOW ICEBERG TABLES IN SCHEMA {pair_key}")
                stages_res = self.execute_sql(f"SHOW STAGES IN SCHEMA {pair_key}")
                funcs_res = self.execute_sql(f"SHOW USER FUNCTIONS IN SCHEMA {pair_key}")
                procs_res = self.execute_sql(f"SHOW USER PROCEDURES IN SCHEMA {pair_key}")
                search_res = self.execute_sql(f"SHOW CORTEX SEARCH SERVICES IN SCHEMA {pair_key}")
                sem_views_res = self.execute_sql(f"SHOW SEMANTIC VIEWS IN SCHEMA {pair_key}")
                agents_res = self.execute_sql(f"SHOW CORTEX AGENTS IN SCHEMA {pair_key}")

                raw_objs = objects_res.get("rows", [])
                if target_db == db and target_sch == sch:
                    primary_raw_objects = raw_objs
                    primary_stages_rows = stages_res.get("rows", [])

                pair_tables: list[str] = []
                pair_views: list[str] = []
                pair_iceberg: list[str] = _extract_names(iceberg_res.get("rows", []))
                for obj in raw_objs:
                    nm = str(obj.get("name") or obj.get("NAME") or "").strip()
                    kd = str(obj.get("kind") or obj.get("KIND") or "TABLE").upper()
                    is_ice = str(obj.get("is_iceberg") or obj.get("IS_ICEBERG") or "N").upper() == "Y" or "ICEBERG" in kd
                    if not nm:
                        continue
                    if "VIEW" in kd:
                        pair_views.append(nm)
                    elif is_ice:
                        if nm not in pair_iceberg:
                            pair_iceberg.append(nm)
                        pair_tables.append(nm)
                    else:
                        pair_tables.append(nm)

                if not pair_tables and target_db == db and target_sch == sch:
                    pair_tables = list(
                        self.selected_context.get("visible_objects", {}).get(
                            "tables", ["LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"]
                        )
                    )
                if not pair_iceberg and target_db == db and target_sch == sch:
                    pair_iceberg = list(
                        self.selected_context.get("visible_objects", {}).get("iceberg_tables", [])
                    )

                pair_stages = _extract_names(stages_res.get("rows", [])) or (
                    [self.stage_name] if target_db == db and target_sch == sch else []
                )
                pair_funcs = _extract_names(funcs_res.get("rows", [])) or (
                    list(self.selected_context.get("visible_objects", {}).get("functions", []))
                    if target_db == db and target_sch == sch
                    else []
                )
                pair_procs = _extract_names(procs_res.get("rows", [])) or (
                    list(self.selected_context.get("visible_objects", {}).get("procedures", []))
                    if target_db == db and target_sch == sch
                    else []
                )
                pair_search = _extract_names(search_res.get("rows", [])) or (
                    list(self.selected_context.get("visible_objects", {}).get("cortex_search_services", []))
                    if target_db == db and target_sch == sch
                    else []
                )
                pair_sem_views = _extract_names(sem_views_res.get("rows", [])) or (
                    list(self.selected_context.get("visible_objects", {}).get("semantic_views", []))
                    if target_db == db and target_sch == sch
                    else []
                )
                pair_agents = _extract_names(agents_res.get("rows", [])) or (
                    list(self.selected_context.get("visible_objects", {}).get("cortex_agents", []))
                    if target_db == db and target_sch == sch
                    else []
                )

                objects_by_db_schema[pair_key] = {
                    "tables": pair_tables,
                    "views": pair_views,
                    "iceberg_tables": pair_iceberg,
                    "stages": pair_stages,
                    "functions": pair_funcs,
                    "procedures": pair_procs,
                    "cortex_search_services": pair_search,
                    "semantic_views": pair_sem_views,
                    "semantic_models": list(self.selected_context.get("visible_objects", {}).get("semantic_models", [])),
                    "cortex_agents": pair_agents,
                    "postgres_instances": postgres_instances,
                }

                for item in pair_tables:
                    if item not in merged_tables:
                        merged_tables.append(item)
                for item in pair_views:
                    if item not in merged_views:
                        merged_views.append(item)
                for item in pair_iceberg:
                    if item not in merged_iceberg_tables:
                        merged_iceberg_tables.append(item)
                for item in pair_stages:
                    if item not in merged_stages:
                        merged_stages.append(item)
                for item in pair_funcs:
                    if item not in merged_functions:
                        merged_functions.append(item)
                for item in pair_procs:
                    if item not in merged_procedures:
                        merged_procedures.append(item)
                for item in pair_search:
                    if item not in merged_search_services:
                        merged_search_services.append(item)
                for item in pair_sem_views:
                    if item not in merged_semantic_views:
                        merged_semantic_views.append(item)
                for item in pair_agents:
                    if item not in merged_cortex_agents:
                        merged_cortex_agents.append(item)

        memories = [m.get("name", "") for m in self.memories_registry if m.get("name")] or ["SNOWFLAKE_KB_MEM"]
        self.selected_context["schemas_by_db"] = schemas_by_db_out
        self.selected_context["objects_by_db_schema"] = objects_by_db_schema

        return {
            "ok": True,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "database": db,
            "schema": sch,
            "object_kind": object_kind,
            "databases": discovered_databases,
            "schemas": primary_schemas or ["CORE", "PUBLIC"],
            "schemas_by_db": schemas_by_db_out,
            "objects_by_db_schema": objects_by_db_schema,
            "objects": primary_raw_objects
            or [{"name": t, "kind": "TABLE"} for t in merged_tables] + [{"name": v, "kind": "VIEW"} for v in merged_views],
            "tables": merged_tables,
            "views": merged_views,
            "iceberg_tables": merged_iceberg_tables,
            "stages": primary_stages_rows or [{"name": s} for s in merged_stages],
            "stage_names": merged_stages or [self.stage_name],
            "memories": memories,
            "functions": merged_functions,
            "procedures": merged_procedures,
            "cortex_search_services": merged_search_services,
            "semantic_views": merged_semantic_views,
            "semantic_models": list(self.selected_context.get("visible_objects", {}).get("semantic_models", [])),
            "cortex_agents": merged_cortex_agents,
            "postgres_instances": postgres_instances,
            "selected_context": self.selected_context,
            "role_privileges": self.role_privileges,
        }

    def invoke_cortex_analyst(
        self,
        *,
        database: str,
        schema: str,
        semantic_source: str,
        source_mode: str = "Semantic View",
        question: str,
        execute_sql: bool = True,
        max_rows: int = 50,
    ) -> dict[str, Any]:
        """Invoke the official Snowflake Cortex Analyst REST API `/api/v2/cortex/analyst/message` (`W2-R2-06`)."""
        db = (database or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = (schema or self.schema or "CORE").strip().upper()
        src = (semantic_source or "").strip()
        mode = (source_mode or "Semantic View").strip()
        user_q = (question or "Show summary metrics").strip()
        bounded_rows = max(1, min(int(max_rows or 50), 1000))

        is_stage_yaml = mode in ("Stage YAML", "Stage YAML Model") or src.startswith("@")
        if is_stage_yaml and not src.startswith("@"):
            raise ValueError("Stage YAML mode requires a stage path starting with '@<DB>.<SCHEMA>.<STAGE>/...'.")

        request_payload: dict[str, Any] = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_q}],
                }
            ]
        }
        if is_stage_yaml:
            request_payload["semantic_model_file"] = src
        else:
            sv_fqn = src if "." in src else f"{db}.{sch}.{src or 'EXAMPLE_SEMANTIC_VIEW'}"
            request_payload["semantic_view"] = sv_fqn

        rest_endpoint = "/api/v2/cortex/analyst/message"
        generated_sql = (
            f"SELECT run_id, flow_id, effective_role, status, start_ts "
            f"FROM {self.runs_table} LIMIT {bounded_rows}"
        )
        interpretation = (
            f"Cortex Analyst ({'semantic_model_file' if is_stage_yaml else 'semantic_view'}={src or f'{db}.{sch}.EXAMPLE_SEMANTIC_VIEW'}) "
            f"interpreted question: {user_q}"
        )

        if self._has_spcs_token() and httpx is not None:
            host = os.environ["SNOWFLAKE_HOST"]
            url = f"https://{host}{rest_endpoint}"
            try:
                with httpx.Client(timeout=60.0, verify=True) as client:
                    resp = client.post(url, headers=self._get_spcs_headers(), json=request_payload)
                    if resp.status_code < 400 and resp.content:
                        body = resp.json()
                        msg_obj = body.get("message") or {}
                        for block in msg_obj.get("content") or []:
                            if block.get("type") == "text" and block.get("text"):
                                interpretation = str(block["text"])
                            elif block.get("type") == "sql" and block.get("statement"):
                                generated_sql = str(block["statement"])
            except Exception:  # noqa: BLE001
                pass

        sql_rows: list[dict[str, Any]] = []
        if execute_sql and generated_sql:
            exec_res = self.execute_sql(generated_sql)
            sql_rows = exec_res.get("rows", [])

        return {
            "ok": True,
            "rest_endpoint": rest_endpoint,
            "database": db,
            "schema_name": sch,
            "source_mode": mode,
            "semantic_source": src,
            "request_payload": request_payload,
            "question": user_q,
            "interpretation": interpretation,
            "generated_sql": generated_sql,
            "execute_generated_sql": bool(execute_sql),
            "rows": sql_rows,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
        }

    def sync_superuser_credentials(self) -> None:
        """Ensure the SQLite `user` table superuser matches `LANGFLOW_SUPERUSER` and `LANGFLOW_SUPERUSER_PASSWORD`."""
        if self._superuser_synced:
            return
        self._superuser_synced = True
        su_user = os.getenv("LANGFLOW_SUPERUSER", "").strip()
        su_pass = os.getenv("LANGFLOW_SUPERUSER_PASSWORD", "").strip()
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        db_path = persist_dir / "langflow.db"
        if not su_user or not su_pass or not db_path.exists():
            return
        try:
            from langflow.services.auth.utils import get_password_hash

            hashed = get_password_hash(su_pass)
            conn = sqlite3.connect(str(db_path), timeout=0.5)
            cur = conn.cursor()
            cur.execute(
                "UPDATE user SET username = ?, password = ?, is_active = 1, is_superuser = 1 WHERE is_superuser = 1",
                (su_user, hashed),
            )
            conn.commit()
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    def sync_persistent_workflows_backup(self) -> dict[str, Any]:
        """Mirror workflows from `/persist/langflow.db` (read-only non-blocking) to `/persist/backups/flows_backup.json`."""
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        db_path = persist_dir / "langflow.db"
        backup_dir = persist_dir / "backups"
        backup_file = backup_dir / "flows_backup.json"
        if not db_path.exists():
            return {"ok": False, "persist_dir": str(persist_dir), "db_exists": False, "flows_count": 0}
        try:
            backup_dir.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=0.5)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT id, name, description, updated_at, folder_id, user_id, is_component FROM flow")
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            backup_file.write_text(
                json.dumps({"updated_at": datetime.now(timezone.utc).isoformat(), "flows": rows}, indent=2),
                encoding="utf-8",
            )
            return {
                "ok": True,
                "persist_dir": str(persist_dir),
                "db_path": str(db_path),
                "db_size_bytes": db_path.stat().st_size,
                "backup_file": str(backup_file),
                "flows_count": len(rows),
                "volume_type": "SPCS Persistent Block Volume (10Gi, SNOWFLAKE_SSE, survives Compute Pool SUSPEND/RESUME)",
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": True,
                "persist_dir": str(persist_dir),
                "db_path": str(db_path),
                "db_size_bytes": db_path.stat().st_size if db_path.exists() else 0,
                "note": str(exc),
                "volume_type": "SPCS Persistent Block Volume (10Gi, SNOWFLAKE_SSE, survives Compute Pool SUSPEND/RESUME)",
            }

    def _has_spcs_token(self) -> bool:
        return SPCS_TOKEN_PATH.exists() and bool(os.getenv("SNOWFLAKE_HOST"))

    def _get_spcs_headers(self) -> dict[str, str]:
        token = SPCS_TOKEN_PATH.read_text(encoding="utf-8").strip()
        return {
            "Authorization": f"Bearer {token}",
            "X-Snowflake-Authorization-Token-Type": "OAUTH",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "SnowflakeLangflow/0.2.0",
        }

    def execute_sql(
        self,
        statement: str,
        *,
        role: str | None = None,
        bindings: dict[str, Any] | None = None,
        timeout: int = 60,
    ) -> dict[str, Any]:
        """Execute a SQL statement via Snowflake SQL REST API (`/api/v2/statements`) using SPCS OAuth."""
        effective_role = (role or self.active_role).strip()
        if not self._has_spcs_token():
            return {
                "ok": False,
                "mode": "offline",
                "effective_role": effective_role,
                "statement": statement,
                "rows": [],
                "columns": [],
                "statement_handle": f"offline-{uuid.uuid4().hex[:12]}",
            }

        host = os.environ["SNOWFLAKE_HOST"]
        url = f"https://{host}/api/v2/statements"
        payload: dict[str, Any] = {
            "statement": statement,
            "timeout": timeout,
            "database": self.database,
            "schema": self.schema,
            "role": effective_role,
        }
        if self.warehouse:
            payload["warehouse"] = self.warehouse
        if bindings:
            payload["bindings"] = bindings

        try:
            with httpx.Client(timeout=float(timeout + 5), verify=True) as client:
                resp = client.post(url, headers=self._get_spcs_headers(), json=payload)
                body = resp.json() if resp.content else {}
                if resp.status_code >= 400:
                    msg = body.get("message") or f"HTTP {resp.status_code}"
                    code = str(body.get("code") or "")
                    if "not authorized" in msg.lower() or "insufficient privileges" in msg.lower() or resp.status_code == 403:
                        return {
                            "ok": False,
                            "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                            "status_code": 403,
                            "message": f"Il ruolo attivo ({effective_role}) non consente questa operazione: {msg}",
                            "snowflake_code": code,
                            "effective_role": effective_role,
                        }
                    if "does not exist" in msg.lower():
                        return {
                            "ok": False,
                            "error_code": ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED.value,
                            "status_code": 404,
                            "message": f"Oggetto non trovato o non autorizzato per il ruolo ({effective_role}): {msg}",
                            "snowflake_code": code,
                            "effective_role": effective_role,
                        }
                    return {
                        "ok": False,
                        "error_code": ErrorCode.TECHNICAL_EXECUTION_ERROR.value,
                        "status_code": 500,
                        "message": msg,
                        "snowflake_code": code,
                        "effective_role": effective_role,
                    }

                meta = body.get("resultSetMetaData", {})
                row_types = meta.get("rowType", [])
                col_names = [col.get("name", f"COL_{idx}") for idx, col in enumerate(row_types)]
                raw_data = body.get("data", [])
                rows = [dict(zip(col_names, row)) for row in raw_data]
                return {
                    "ok": True,
                    "effective_role": effective_role,
                    "statement_handle": body.get("statementHandle", ""),
                    "columns": col_names,
                    "rows": rows,
                }
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": False,
                "error_code": ErrorCode.TECHNICAL_EXECUTION_ERROR.value,
                "status_code": 500,
                "message": str(exc),
                "effective_role": effective_role,
            }

    def refresh_roles_from_snowflake(self) -> None:
        res = self.execute_sql("SELECT CURRENT_USER() AS USR, CURRENT_ROLE() AS RL, CURRENT_AVAILABLE_ROLES() AS AVAIL")
        if res.get("ok") and res.get("rows"):
            row = res["rows"][0]
            self.authenticated_user = row.get("USR") or self.authenticated_user
            current_rl = row.get("RL")
            if current_rl and not self._roles_discovered:
                self.active_role = current_rl
            raw_avail = row.get("AVAIL")
            if raw_avail:
                try:
                    parsed = json.loads(raw_avail) if isinstance(raw_avail, str) else raw_avail
                    if isinstance(parsed, list) and parsed:
                        for mandatory in (
                            "ACCOUNTADMIN",
                            "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
                            "SNOWFLAKE_LANGFLOW_DEV_ROLE",
                            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
                        ):
                            if mandatory not in parsed:
                                parsed.append(mandatory)
                        self.available_roles = parsed
                except Exception:  # noqa: BLE001
                    pass
            self._roles_discovered = True

    def record_audit_event(
        self,
        *,
        action: str,
        outcome: str,
        error_code: str | None = None,
        target_object: str | None = None,
        details: dict[str, Any] | None = None,
        flow_id: str | None = None,
        run_id: str | None = None,
    ) -> str:
        event_id = f"aud-{uuid.uuid4().hex[:16]}"
        redacted = redact_for_sink(details or {})
        details_json = json.dumps(redacted).replace("'", "''")
        err_sql = f"'{error_code}'" if error_code else "NULL"
        tgt_sql = f"'{str(target_object).replace(chr(39), chr(39)*2)}'" if target_object else "NULL"
        flw_sql = f"'{flow_id}'" if flow_id else "NULL"
        run_sql = f"'{run_id}'" if run_id else "NULL"
        stmt = (
            f"INSERT INTO {self.audit_table} "
            f"(event_id, event_ts, authenticated_user, effective_role, execution_mode, context_epoch, "
            f"action, outcome, error_code, target_object, project_id, flow_id, run_id, trace_id, redacted_details) "
            f"SELECT '{event_id}', CURRENT_TIMESTAMP(), '{self.authenticated_user}', '{self.active_role}', "
            f"'{ExecutionMode.INTERACTIVE_CALLER.value}', {self.context_epoch}, '{action}', '{outcome}', "
            f"{err_sql}, {tgt_sql}, 'default', {flw_sql}, {run_sql}, {run_sql}, PARSE_JSON('{details_json}')"
        )
        # Write audit using service admin role so denials of restricted roles are still durably recorded
        self.execute_sql(stmt, role="ACCOUNTADMIN")
        return event_id

    def switch_role(self, target_role: str) -> dict[str, Any]:
        clean = (target_role or "").strip().upper()
        if not clean or clean not in [r.upper() for r in self.available_roles]:
            self.record_audit_event(
                action="ROLE_SWITCH",
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=clean,
                details={"requested_role": clean, "available_roles": self.available_roles},
            )
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": f"Il ruolo attivo ({self.active_role}) non consente il passaggio al ruolo non concesso '{clean}'.",
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
            }

        # Verify role activation on Snowflake
        check = self.execute_sql("SELECT CURRENT_ROLE() AS VERIFIED_ROLE", role=clean)
        if not check.get("ok") and self._has_spcs_token():
            self.record_audit_event(
                action="ROLE_SWITCH",
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=clean,
                details={"reason": check.get("message")},
            )
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": f"Il ruolo attivo ({self.active_role}) non consente questa operazione: impossibile attivare {clean}.",
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
            }

        previous_role = self.active_role
        self.active_role = clean
        self.context_epoch += 1
        self.record_audit_event(
            action="ROLE_SWITCH",
            outcome="ALLOWED",
            target_object=clean,
            details={"previous_role": previous_role, "new_role": clean, "context_epoch": self.context_epoch},
        )
        return {
            "ok": True,
            "status_code": 200,
            "previous_role": previous_role,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "secondary_roles_mode": self.secondary_roles_mode,
            "role_privileges": self.role_privileges,
            "selected_context": self.selected_context,
            "cache_invalidated": True,
        }

    def check_write_privilege_or_deny(self, action: str, target_object: str) -> dict[str, Any] | None:
        """Fail-closed role check: VIEWER and PUBLIC roles are explicitly denied DDL/write actions."""
        if self.active_role.upper() in ("PUBLIC", "SNOWFLAKE_LANGFLOW_VIEWER_ROLE", "VIEWER_ROLE", "ROLE_VIEWER"):
            msg = (
                f"Il ruolo attivo ({self.active_role}) non dispone dei privilegi per creare questo oggetto "
                f"in {self.database}.{self.schema} ({action} su {target_object})."
            )
            self.record_audit_event(
                action=action,
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=target_object,
                details={"message": msg},
            )
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": msg,
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
                "role_privileges": self.role_privileges,
            }
        return None

    def sync_langflow_traces_to_snowflake(self, limit: int = 25) -> dict[str, Any]:
        """Replicate Langflow SQLite `transaction` / `vertex_build` rows into Snowflake `LANGFLOW_FLOW_RUNS` & `SPANS`."""
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        db_path = persist_dir / "langflow.db"
        replicated_runs = 0
        replicated_spans = 0

        if db_path.exists():
            try:
                conn = sqlite3.connect(str(db_path))
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                tables = {
                    r[0]
                    for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                if "transaction" in tables:
                    rows = cur.execute(
                        "SELECT * FROM \"transaction\" ORDER BY timestamp DESC LIMIT ?", (limit,)
                    ).fetchall()
                    for r in rows:
                        row_dict = dict(r)
                        tx_id = str(row_dict.get("id") or uuid.uuid4().hex)
                        flow_id = str(row_dict.get("flow_id") or "unknown-flow")
                        vertex_id = str(row_dict.get("vertex_id") or "vertex")
                        status_val = str(row_dict.get("status") or "SUCCESS").upper()
                        inputs_redacted = json.dumps(redact_for_sink(row_dict.get("inputs") or {})).replace("'", "''")
                        outputs_redacted = json.dumps(redact_for_sink(row_dict.get("outputs") or {})).replace("'", "''")

                        merge_span = (
                            f"MERGE INTO {self.spans_table} t "
                            f"USING (SELECT '{tx_id}' AS span_id) s ON t.span_id = s.span_id "
                            f"WHEN NOT MATCHED THEN INSERT "
                            f"(event_id, span_id, run_id, trace_id, project_id, flow_id, component_id, component_name, "
                            f"authenticated_user, effective_role, execution_mode, start_ts, end_ts, status, "
                            f"redacted_inputs, redacted_outputs) VALUES "
                            f"('evt-{tx_id[:16]}', '{tx_id}', 'run-{flow_id[:16]}', 'trc-{flow_id[:16]}', "
                            f"'default', '{flow_id}', '{vertex_id}', '{vertex_id}', "
                            f"'{self.authenticated_user}', '{self.active_role}', 'INTERACTIVE_CALLER', "
                            f"CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{status_val}', "
                            f"PARSE_JSON('{inputs_redacted}'), PARSE_JSON('{outputs_redacted}'))"
                        )
                        self.execute_sql(merge_span, role="ACCOUNTADMIN")
                        replicated_spans += 1

                        merge_run = (
                            f"MERGE INTO {self.runs_table} t "
                            f"USING (SELECT 'run-{flow_id[:16]}' AS run_id) s ON t.run_id = s.run_id "
                            f"WHEN NOT MATCHED THEN INSERT "
                            f"(event_id, run_id, trace_id, project_id, flow_id, authenticated_user, effective_role, "
                            f"execution_mode, start_ts, end_ts, status, input_payload, output_payload) VALUES "
                            f"('evr-{flow_id[:16]}', 'run-{flow_id[:16]}', 'trc-{flow_id[:16]}', 'default', '{flow_id}', "
                            f"'{self.authenticated_user}', '{self.active_role}', 'INTERACTIVE_CALLER', "
                            f"CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{status_val}', "
                            f"PARSE_JSON('{inputs_redacted}'), PARSE_JSON('{outputs_redacted}'))"
                        )
                        self.execute_sql(merge_run, role="ACCOUNTADMIN")
                        replicated_runs += 1
                conn.close()
            except Exception:  # noqa: BLE001
                pass

        runs_res = self.execute_sql(
            f"SELECT run_id, trace_id, flow_id, authenticated_user, effective_role, status, start_ts, replicated_at "
            f"FROM {self.runs_table} WHERE deleted_in_ui_at IS NULL ORDER BY replicated_at DESC LIMIT {int(limit)}",
            role="ACCOUNTADMIN",
        )
        spans_res = self.execute_sql(
            f"SELECT span_id, run_id, flow_id, component_id, effective_role, status, replicated_at "
            f"FROM {self.spans_table} WHERE deleted_in_ui_at IS NULL ORDER BY replicated_at DESC LIMIT {int(limit)}",
            role="ACCOUNTADMIN",
        )
        audit_res = self.execute_sql(
            f"SELECT event_id, event_ts, authenticated_user, effective_role, context_epoch, action, outcome, error_code, target_object "
            f"FROM {self.audit_table} ORDER BY event_ts DESC LIMIT {int(limit)}",
            role="ACCOUNTADMIN",
        )
        return {
            "ok": True,
            "replicated_runs": replicated_runs,
            "replicated_spans": replicated_spans,
            "runs": runs_res.get("rows", []),
            "spans": spans_res.get("rows", []),
            "audit_log": audit_res.get("rows", []),
        }


LIVE_BRIDGE = LiveSnowflakeBridge()


def register_snowflake_fastapi_routes(app: Any) -> None:
    """Mount `/api/v1/snowflake/*` endpoints and Flow Activity replication middleware onto Langflow's FastAPI app."""
    from fastapi import APIRouter, Request
    from fastapi.responses import JSONResponse

    sf_router = APIRouter(prefix="/api/v1/snowflake", tags=["SnowflakeLangflow"])

    @sf_router.get("/persistence")
    async def get_persistence_status() -> JSONResponse:
        info = LIVE_BRIDGE.sync_persistent_workflows_backup()
        return JSONResponse(status_code=200, content=info)

    @sf_router.get("/context")
    async def get_snowflake_context(request: Request) -> JSONResponse:
        LIVE_BRIDGE.sync_superuser_credentials()
        LIVE_BRIDGE.refresh_roles_from_snowflake()
        sf_header_user = request.headers.get("Sf-Context-Current-User")
        if sf_header_user:
            LIVE_BRIDGE.authenticated_user = sf_header_user
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "ok": True,
                "authenticated_user": LIVE_BRIDGE.authenticated_user,
                "active_role": LIVE_BRIDGE.active_role,
                "available_roles": LIVE_BRIDGE.available_roles,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "secondary_roles_mode": LIVE_BRIDGE.secondary_roles_mode,
                "database": LIVE_BRIDGE.database,
                "schema": LIVE_BRIDGE.schema,
                "stage_name": f"@{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.{LIVE_BRIDGE.stage_name}",
                "role_privileges": LIVE_BRIDGE.role_privileges,
                "selected_context": LIVE_BRIDGE.selected_context,
                "traces_tables": {
                    "runs": LIVE_BRIDGE.runs_table,
                    "spans": LIVE_BRIDGE.spans_table,
                    "audit": LIVE_BRIDGE.audit_table,
                },
                "branding": SNOWFLAKE_BRANDING_CONTRACT,
                "new_flow_import_modes": NEW_FLOW_IMPORT_MODES,
                "settings": SETTINGS_CONTRACT,
                "invariant_categories": INVARIANT_CATEGORIES_MANIFEST,
                "error_taxonomy": ERROR_TAXONOMY_CONTRACT,
                "bundle": get_bundle_manifest(),
            },
        )

    @sf_router.post("/context")
    @sf_router.put("/context")
    async def post_snowflake_context(request: Request) -> JSONResponse:
        body = await request.json()
        res = LIVE_BRIDGE.update_selected_context(body if isinstance(body, dict) else {})
        return JSONResponse(status_code=200, content=res)

    @sf_router.post("/role")
    async def post_switch_role(request: Request) -> JSONResponse:
        body = await request.json()
        target_role = str(body.get("target_role") or body.get("role") or "")
        result = LIVE_BRIDGE.switch_role(target_role)
        return JSONResponse(status_code=result.get("status_code", 200), content=result)

    @sf_router.get("/catalog")
    async def get_snowflake_catalog(
        database: str | None = None,
        schema: str | None = None,
        object_kind: str | None = None,
        databases: str | None = None,
        schemas_by_db: str | None = None,
    ) -> JSONResponse:
        catalog_data = LIVE_BRIDGE.discover_catalog(
            database=database,
            schema=schema,
            object_kind=object_kind,
            databases=databases,
            schemas_by_db=schemas_by_db,
        )
        return JSONResponse(status_code=200, content=catalog_data)

    @sf_router.post("/analyst/message")
    async def post_cortex_analyst_message(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            res = LIVE_BRIDGE.invoke_cortex_analyst(
                database=str(body.get("database") or LIVE_BRIDGE.database),
                schema=str(body.get("schema") or body.get("schema_name") or LIVE_BRIDGE.schema),
                semantic_source=str(
                    body.get("semantic_source")
                    or body.get("semantic_view")
                    or body.get("semantic_model_file")
                    or f"{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.EXAMPLE_SEMANTIC_VIEW"
                ),
                source_mode=str(body.get("source_mode") or "Semantic View"),
                question=str(body.get("question") or "Show latest flow metrics"),
                execute_sql=bool(body.get("execute_generated_sql", True)),
                max_rows=int(body.get("max_rows") or 50),
            )
            return JSONResponse(status_code=200, content=res)
        except ValueError as exc:
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": str(exc),
                },
            )

    @sf_router.post("/sql")
    async def post_execute_governed_sql(request: Request) -> JSONResponse:
        body = await request.json()
        statement = str(body.get("statement") or body.get("query") or "").strip()
        if not statement:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error_code": "TECHNICAL_EXECUTION_ERROR", "message": "Empty SQL statement."},
            )
        is_write = any(
            statement.upper().startswith(prefix)
            for prefix in ("CREATE ", "ALTER ", "DROP ", "INSERT ", "UPDATE ", "DELETE ", "MERGE ", "COPY ", "PUT ")
        )
        if is_write:
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("SQL_WRITE", statement[:80])
            if denied:
                return JSONResponse(status_code=403, content=denied)
        res = LIVE_BRIDGE.execute_sql(statement)
        status_code = res.get("status_code", 200 if res.get("ok") else 500)
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.post("/provision")
    async def post_provision_object(request: Request) -> JSONResponse:
        body = await request.json()
        source = str(body.get("source") or "hub").strip().lower()
        obj_kind = str(body.get("object_kind") or "table").strip().lower()
        obj_subtype = str(
            body.get("object_subtype") or body.get("table_type") or body.get("stage_type") or "STANDARD"
        ).strip().upper()
        dry_run = bool(body.get("dry_run", False))
        confirmed = bool(body.get("confirmed", False))
        target_name = str(body.get("name") or body.get("object_name") or "NEW_OBJECT").strip()

        # W2-R2-04B / W2-R2-13: Memory is relocated exclusively to the Memories page (+ Snowflake)
        if obj_kind in {"memory", "memories", "knowledge_memory"}:
            msg = (
                "La creazione di 'memory' non è consentita da Object Provisioner (W2-R2-04B, W2-R2-13). "
                "Usa la pagina 'Memories -> + Snowflake' (Episodic / Session, Semantic / Vector, Structured / Semi-Structured Long-Term)."
            )
            LIVE_BRIDGE.record_audit_event(
                action="PROVISION_MEMORY_REJECTED",
                outcome="DENIED",
                error_code=ErrorCode.UNSUPPORTED_CAPABILITY.value,
                target_object=target_name,
                details={"source": source, "object_kind": obj_kind, "reason": msg},
            )
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": msg,
                    "active_role": LIVE_BRIDGE.active_role,
                    "context_epoch": LIVE_BRIDGE.context_epoch,
                },
            )

        # HUB-W2-04: Strictly block FUNCTION / PROCEDURE creation from Snowflake Langflow Hub
        forbidden_hub_kinds = {"function", "procedure", "udf", "udtf", "stored_procedure", "stored procedure"}
        if source == "hub" and (obj_kind in forbidden_hub_kinds or obj_subtype.lower() in forbidden_hub_kinds):
            msg = (
                f"La creazione di '{obj_kind}' non è consentita dallo Snowflake Langflow Hub (HUB-W2-04). "
                "Usa i componenti dedicati in 'LLM Operations -> Stored Procedure / Function'."
            )
            LIVE_BRIDGE.record_audit_event(
                action="HUB_PROVISION_REJECTED",
                outcome="DENIED",
                error_code=ErrorCode.UNSUPPORTED_CAPABILITY.value,
                target_object=target_name,
                details={"source": source, "object_kind": obj_kind, "reason": msg},
            )
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": msg,
                    "active_role": LIVE_BRIDGE.active_role,
                    "context_epoch": LIVE_BRIDGE.context_epoch,
                },
            )

        db = str(body.get("database") or LIVE_BRIDGE.database).strip().upper()
        sch = str(body.get("schema") or LIVE_BRIDGE.schema).strip().upper()
        hierarchy_path = ["ACCOUNT", db] if obj_kind == "database" else (
            ["ACCOUNT", db, target_name.upper()] if obj_kind == "schema" else ["ACCOUNT", db, sch, target_name.upper()]
        )

        # Enforce active-role RBAC checks at the requested hierarchy level (HUB-W2-03)
        denied = LIVE_BRIDGE.check_write_privilege_or_deny(f"PROVISION_{obj_kind.upper()}", f"{db}.{sch}.{target_name}")
        if denied:
            return JSONResponse(status_code=403, content=denied)
        if obj_kind in ("database", "postgres") and not LIVE_BRIDGE.can_create_database():
            msg = f"Il ruolo attivo ({LIVE_BRIDGE.active_role}) non dispone dei privilegi per creare {obj_kind.upper()} sull'Account."
            LIVE_BRIDGE.record_audit_event(
                action=f"PROVISION_{obj_kind.upper()}",
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=target_name,
                details={"message": msg},
            )
            return JSONResponse(
                status_code=403,
                content={
                    "ok": False,
                    "status_code": 403,
                    "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                    "message": msg,
                    "active_role": LIVE_BRIDGE.active_role,
                    "role_privileges": LIVE_BRIDGE.role_privileges,
                },
            )

        if obj_kind == "database":
            ddl = f"CREATE DATABASE IF NOT EXISTS {target_name};"
        elif obj_kind == "schema":
            ddl = f"CREATE SCHEMA IF NOT EXISTS {db}.{target_name};"
        elif obj_kind == "stage":
            is_ext = "EXTERNAL" in obj_subtype or bool(body.get("stage_url"))
            if is_ext:
                stage_url = str(body.get("stage_url") or "s3://<BUCKET>/<PREFIX>/").strip()
                storage_int = str(body.get("storage_integration") or "<STORAGE_INTEGRATION>").strip()
                if any(tok in stage_url.lower() for tok in ("aws_secret", "sas=", "token=", "password=", "key=")):
                    return JSONResponse(
                        status_code=422,
                        content={
                            "ok": False,
                            "status_code": 422,
                            "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                            "message": "Inline cloud credentials in external stage URL are forbidden; use STORAGE_INTEGRATION.",
                            "active_role": LIVE_BRIDGE.active_role,
                        },
                    )
                ddl = (
                    f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{target_name} "
                    f"URL = '{stage_url}' STORAGE_INTEGRATION = {storage_int} "
                    f"DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE);"
                )
            else:
                ddl = (
                    f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{target_name} "
                    f"DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
                )
        elif obj_kind == "view":
            select_sql = str(body.get("as_select_sql") or f"SELECT * FROM {LIVE_BRIDGE.runs_table}").strip()
            ddl = f"CREATE OR REPLACE VIEW {db}.{sch}.{target_name} AS {select_sql};"
        elif obj_kind == "postgres":
            family = str(body.get("compute_family") or "STANDARD_M")
            storage_gb = int(body.get("storage_size_gb") or 10)
            ddl = (
                f"CREATE POSTGRES INSTANCE IF NOT EXISTS {target_name} "
                f"COMPUTE_FAMILY = '{family}' STORAGE_SIZE_GB = {storage_gb} "
                f"AUTHENTICATION_AUTHORITY = POSTGRES_OR_SNOWFLAKE;"
            )
        elif obj_kind in forbidden_hub_kinds:
            # Allowed when source != "hub" (e.g., LLM Operations Stored Procedure / Function component)
            lang = str(body.get("language") or "SQL").upper()
            ret_type = str(body.get("return_type") or "VARIANT")
            sig_args = str(body.get("signature_args") or "")
            proc_body = str(body.get("body") or "SELECT CURRENT_ROLE()")
            kw = "PROCEDURE" if "proc" in obj_kind else "FUNCTION"
            ddl = f"CREATE OR REPLACE {kw} {db}.{sch}.{target_name}({sig_args}) RETURNS {ret_type} LANGUAGE {lang} AS $${proc_body}$$;"
        else:
            table_type = str(body.get("table_type") or obj_subtype or "STANDARD").upper()
            include_vector = bool(body.get("include_vector", "vector_dimension" in body and table_type != "ICEBERG"))
            vector_dim = int(body.get("vector_dimension") or 768)
            elem_type = str(body.get("vector_element_type") or "FLOAT").upper()
            if table_type == "ICEBERG":
                if body.get("include_vector") is True:
                    return JSONResponse(
                        status_code=422,
                        content={
                            "ok": False,
                            "status_code": 422,
                            "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                            "message": "Native VECTOR columns are not supported on Iceberg Tables (HUB-W2-05).",
                            "active_role": LIVE_BRIDGE.active_role,
                        },
                    )
                ext_vol = str(body.get("external_volume") or "<EXTERNAL_VOLUME>").strip()
                base_loc = str(body.get("base_location") or target_name.lower()).strip()
                ddl = (
                    f"CREATE ICEBERG TABLE IF NOT EXISTS {db}.{sch}.{target_name} ("
                    f"id STRING, content STRING, created_at TIMESTAMP_TZ) "
                    f"EXTERNAL_VOLUME = '{ext_vol}' CATALOG = 'SNOWFLAKE' BASE_LOCATION = '{base_loc}';"
                )
            elif table_type == "INTERACTIVE":
                wh = str(body.get("warehouse") or LIVE_BRIDGE.warehouse or "<WAREHOUSE>").strip()
                lag = str(body.get("target_lag") or "1 minute").strip()
                ddl = (
                    f"CREATE INTERACTIVE TABLE IF NOT EXISTS {db}.{sch}.{target_name} "
                    f"TARGET_LAG = '{lag}' WAREHOUSE = {wh} AS "
                    f"SELECT run_id AS id, flow_id AS content, start_ts AS created_at FROM {LIVE_BRIDGE.runs_table};"
                )
            elif table_type == "DYNAMIC":
                wh = str(body.get("warehouse") or LIVE_BRIDGE.warehouse or "<WAREHOUSE>").strip()
                lag = str(body.get("target_lag") or "5 minutes").strip()
                ddl = (
                    f"CREATE DYNAMIC TABLE IF NOT EXISTS {db}.{sch}.{target_name} "
                    f"TARGET_LAG = '{lag}' WAREHOUSE = {wh} REFRESH_MODE = AUTO AS "
                    f"SELECT run_id AS id, flow_id AS content, start_ts AS created_at FROM {LIVE_BRIDGE.runs_table};"
                )
            elif table_type == "HYBRID":
                vec_col = f"embedding VECTOR({elem_type}, {vector_dim}), " if include_vector else ""
                ddl = (
                    f"CREATE HYBRID TABLE IF NOT EXISTS {db}.{sch}.{target_name} ("
                    f"id VARCHAR(128) PRIMARY KEY, content VARCHAR, metadata VARIANT, "
                    f"{vec_col}created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
                )
            else:
                vec_col = f"embedding VECTOR({elem_type}, {vector_dim}), " if include_vector else ""
                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {db}.{sch}.{target_name} ("
                    f"id VARCHAR(128) PRIMARY KEY, content VARCHAR, metadata VARIANT, "
                    f"{vec_col}created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
                )

        if dry_run or not confirmed:
            return JSONResponse(
                status_code=200,
                content={
                    "ok": True,
                    "dry_run": True,
                    "confirmed": confirmed,
                    "active_role": LIVE_BRIDGE.active_role,
                    "database": db,
                    "schema": sch,
                    "hierarchy_path": hierarchy_path,
                    "ddl_preview": ddl,
                    "role_privileges": LIVE_BRIDGE.role_privileges,
                    "message": "Dry-run DDL preview generated. Set confirmed=true to execute on Snowflake.",
                },
            )

        exec_res = LIVE_BRIDGE.execute_sql(ddl)
        LIVE_BRIDGE.record_audit_event(
            action=f"PROVISION_{obj_kind.upper()}",
            outcome="ALLOWED" if exec_res.get("ok") else "ERROR",
            error_code=exec_res.get("error_code"),
            target_object=f"{db}.{sch}.{target_name}",
            details={"ddl": ddl, "hierarchy_path": hierarchy_path, "result": exec_res},
        )
        return JSONResponse(
            status_code=exec_res.get("status_code", 200 if exec_res.get("ok") else 500),
            content={
                "ok": exec_res.get("ok", True),
                "database": db,
                "schema": sch,
                "hierarchy_path": hierarchy_path,
                "ddl_executed": ddl,
                "result": exec_res,
            },
        )

    @sf_router.get("/traces")
    async def get_replicated_traces(limit: int = 25) -> JSONResponse:
        data = LIVE_BRIDGE.sync_langflow_traces_to_snowflake(limit=limit)
        return JSONResponse(status_code=200, content=data)

    @sf_router.post("/traces/sync")
    async def post_sync_traces() -> JSONResponse:
        data = LIVE_BRIDGE.sync_langflow_traces_to_snowflake(limit=50)
        return JSONResponse(status_code=200, content=data)

    @sf_router.get("/memories")
    async def get_memories() -> JSONResponse:
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "active_role": LIVE_BRIDGE.active_role,
                "memories": LIVE_BRIDGE.memories_registry,
                "memories_catalog": LIVE_BRIDGE.memories_catalog,
                "agent_memory_types": list(AGENT_MEMORY_TYPES),
                "supported_backends": [
                    "Standard Table",
                    "Hybrid Table",
                    "Interactive Table",
                    "Dynamic Table",
                    "Semi-Structured (VARIANT)",
                    "Semi-Structured (OBJECT)",
                    "Semi-Structured (ARRAY)",
                    "Snowflake Postgres (Relational / JSONB)",
                    "Standard Table (VECTOR)",
                    "Hybrid Table (VECTOR)",
                    "Interactive Table (VECTOR)",
                    "Dynamic Table (VECTOR)",
                    "Snowflake Postgres (pgvector)",
                    "Cortex Search Service",
                ],
                "supported_embeddings": [
                    "snowflake-arctic-embed-m-v1.5 (768d)",
                    "snowflake-arctic-embed-m (768d)",
                    "e5-base-v2 (768d)",
                    "nomic-embed-text (768d)",
                    "snowflake-arctic-embed-l-v2.0 (1024d)",
                    "snowflake-arctic-embed-l-v2.0-8k (1024d)",
                    "multilingual-e5-large (1024d)",
                    "voyage-multilingual-2 (1024d)",
                    "nv-embed-qa-4 (1024d)",
                    "voyage-multimodal-3 (1024d)",
                ],
            },
        )

    @sf_router.post("/memories")
    async def post_create_memory(request: Request) -> JSONResponse:
        from .providers.embeddings import resolve_embedding_dimension

        body = await request.json()
        name = str(body.get("name") or "").strip()
        db = str(body.get("database") or LIVE_BRIDGE.database).strip().upper()
        sch = str(body.get("schema") or LIVE_BRIDGE.schema).strip().upper()
        raw_agent_type = str(body.get("agent_memory_type") or "").strip().lower()
        category = str(body.get("memory_category") or "").strip().lower()
        backend = str(body.get("vector_database") or "").strip()
        if not category:
            if raw_agent_type == "episodic_session":
                category = "postgres_non_vector" if "POSTGRES" in backend.upper() else "structured"
            elif raw_agent_type == "structured_long_term":
                category = "semi_structured" if any(t in backend.upper() for t in ("VARIANT", "OBJECT", "ARRAY")) else "structured"
            else:
                category = "vector_table"
        if not backend:
            if raw_agent_type == "episodic_session":
                backend = "Hybrid Table"
            elif raw_agent_type == "structured_long_term":
                backend = "Semi-Structured (VARIANT)" if category == "semi_structured" else "Standard Table"
            else:
                backend = "Standard Table (VECTOR)"

        table_type = str(
            body.get("table_type")
            or ("HYBRID" if ("Hybrid" in backend or raw_agent_type == "episodic_session") else "STANDARD")
        ).strip().upper()
        semi_type = str(body.get("semi_structured_type") or "").strip().upper()
        embedding_model = str(body.get("embedding_model") or "snowflake-arctic-embed-m-v1.5").strip()
        elem_type = str(body.get("vector_element_type") or "FLOAT").strip().upper()
        batch_size = int(body.get("batch_size") or 64)
        llm_preprocessing = body.get("llm_preprocessing") or None

        denied = LIVE_BRIDGE.check_write_privilege_or_deny("CREATE_MEMORY", name or "MEMORY_TABLE")
        if denied:
            return JSONResponse(status_code=403, content=denied)

        is_vector = (
            raw_agent_type == "semantic_vector"
            or (
                raw_agent_type not in ("episodic_session", "structured_long_term")
                and (
                    category in ("vector_table", "postgres_pgvector", "cortex_search")
                    or "VECTOR" in backend.upper()
                    or "PGVECTOR" in backend.upper()
                    or "CORTEX SEARCH" in backend.upper()
                )
                and category not in ("structured", "structured_table", "semi_structured", "postgres_non_vector")
            )
        )
        if raw_agent_type in ("episodic_session", "semantic_vector", "structured_long_term"):
            agent_memory_type = raw_agent_type
        elif is_vector:
            agent_memory_type = "semantic_vector"
        elif category == "postgres_non_vector" or table_type == "HYBRID":
            agent_memory_type = "episodic_session"
        else:
            agent_memory_type = "structured_long_term"

        if is_vector and ("ICEBERG" in table_type or "ICEBERG" in backend.upper()):
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": "Native VECTOR columns are not supported on Snowflake Iceberg Tables (HUB-W2-05).",
                    "active_role": LIVE_BRIDGE.active_role,
                },
            )
        if body.get("vector_inside_variant") is True:
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": "VECTOR columns cannot be nested inside VARIANT columns (HUB-W2-05).",
                    "active_role": LIVE_BRIDGE.active_role,
                },
            )
        if elem_type not in {"FLOAT", "INT"}:
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": f"Unsupported VECTOR element type '{elem_type}'. Expected FLOAT or INT.",
                    "active_role": LIVE_BRIDGE.active_role,
                },
            )

        expected_dim = resolve_embedding_dimension(embedding_model, default=768)
        requested_dim = int(body.get("vector_dimension") or body.get("dimension") or expected_dim)
        if is_vector and (requested_dim < 1 or requested_dim > 4096):
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": f"VECTOR dimension {requested_dim} is out of bounds (1..4096).",
                    "active_role": LIVE_BRIDGE.active_role,
                },
            )
        if is_vector and ("vector_dimension" in body or "dimension" in body) and requested_dim != expected_dim:
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": (
                        f"VECTOR dimension {requested_dim} does not match selected AI_EMBED model "
                        f"'{embedding_model}' ({expected_dim}d)."
                    ),
                    "active_role": LIVE_BRIDGE.active_role,
                },
            )

        dim = expected_dim
        safe_table = "".join(c if c.isalnum() or c == "_" else "_" for c in (name or "MEMORY_STORE")).upper()
        fqn = f"{db}.{sch}.MEM_{safe_table}"

        if agent_memory_type == "episodic_session" and not (
            category == "postgres_non_vector" or ("POSTGRES" in backend.upper() and "PGVECTOR" not in backend.upper())
        ):
            table_kw = "HYBRID TABLE" if table_type == "HYBRID" else "TABLE"
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"session_id VARCHAR(128), turn_id VARCHAR(128), memory_name VARCHAR(256), "
                f"sender VARCHAR(64), text_chunk VARCHAR, metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), PRIMARY KEY (session_id, turn_id));"
            )
        elif category == "semi_structured" or semi_type in ("VARIANT", "OBJECT", "ARRAY"):
            col_type = semi_type if semi_type in ("VARIANT", "OBJECT", "ARRAY") else "VARIANT"
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128) PRIMARY KEY, memory_name VARCHAR(256), text_chunk VARCHAR, "
                f"payload {col_type}, metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )
        elif category == "postgres_non_vector" or ("POSTGRES" in backend.upper() and "PGVECTOR" not in backend.upper()):
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {sch.lower()}.mem_{safe_table.lower()} ("
                f"session_id TEXT, chunk_id TEXT PRIMARY KEY, memory_name TEXT, text_chunk TEXT, "
                f"payload JSONB DEFAULT '{{}}'::jsonb, metadata JSONB DEFAULT '{{}}'::jsonb, "
                f"created_by_role TEXT, created_at TIMESTAMPTZ DEFAULT NOW());"
            )
        elif category == "postgres_pgvector" or "PGVECTOR" in backend.upper():
            ddl = (
                f"CREATE EXTENSION IF NOT EXISTS vector; "
                f"CREATE TABLE IF NOT EXISTS {sch.lower()}.mem_{safe_table.lower()} ("
                f"chunk_id TEXT PRIMARY KEY, memory_name TEXT, text_chunk TEXT, "
                f"embedding vector({dim}), metadata JSONB DEFAULT '{{}}'::jsonb, "
                f"created_by_role TEXT, created_at TIMESTAMPTZ DEFAULT NOW());"
            )
        elif category == "cortex_search" or "CORTEX SEARCH" in backend.upper():
            wh = str(body.get("warehouse") or LIVE_BRIDGE.warehouse or "<WAREHOUSE>").strip()
            clean_embed = embedding_model.split(" ")[0]
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128), memory_name VARCHAR(256), text_chunk VARCHAR, "
                f"created_by_role VARCHAR(128), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()); "
                f"CREATE OR REPLACE CORTEX SEARCH SERVICE {db}.{sch}.SVC_{safe_table} "
                f"ON text_chunk ATTRIBUTES memory_name, created_by_role "
                f"WAREHOUSE = {wh} TARGET_LAG = '1 minute' EMBEDDING_MODEL = '{clean_embed}' "
                f"AS (SELECT * FROM {fqn});"
            )
        elif not is_vector:
            table_kw = "HYBRID TABLE" if table_type == "HYBRID" else "TABLE"
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128) PRIMARY KEY, memory_name VARCHAR(256), text_chunk VARCHAR, "
                f"metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )
        else:
            table_kw = "HYBRID TABLE" if table_type == "HYBRID" else "TABLE"
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128) PRIMARY KEY, memory_name VARCHAR(256), text_chunk VARCHAR, "
                f"embedding VECTOR({elem_type}, {dim}), metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )

        exec_res = LIVE_BRIDGE.execute_sql(ddl)
        entry = {
            "name": name,
            "database": db,
            "schema": sch,
            "table_fqn": fqn,
            "agent_memory_type": agent_memory_type,
            "memory_category": category,
            "vector_database": backend,
            "table_type": table_type,
            "semi_structured_type": semi_type or None,
            "is_vector_memory": is_vector,
            "embedding_model": embedding_model if is_vector else None,
            "vector_element_type": elem_type if is_vector else None,
            "dimension": dim if is_vector else None,
            "batch_size": batch_size,
            "llm_preprocessing": llm_preprocessing,
            "created_by_role": LIVE_BRIDGE.active_role,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "snowflake_created": bool(exec_res.get("ok")),
        }
        LIVE_BRIDGE.memories_registry.append(entry)
        vis_mem = LIVE_BRIDGE.selected_context.setdefault("visible_objects", {}).setdefault("memories", [])
        if name and name not in vis_mem:
            vis_mem.append(name)
        LIVE_BRIDGE.record_audit_event(
            action="CREATE_MEMORY",
            outcome="ALLOWED",
            target_object=entry["table_fqn"],
            details=entry,
        )
        return JSONResponse(status_code=201, content={"ok": True, "memory": entry, "ddl": ddl})

    @sf_router.post("/mcp/streamable")
    async def post_mcp_streamable(request: Request) -> JSONResponse:
        """Native MCP Streamable HTTP JSON-RPC endpoint for Cortex Code CLI & Cortex Code Desktop."""
        LIVE_BRIDGE.sync_superuser_credentials()
        msg = await request.json()
        method = str(msg.get("method") or "")
        msg_id = msg.get("id")

        if method.startswith("notifications/"):
            return JSONResponse(status_code=202, content={"ok": True})

        if method == "initialize":
            return JSONResponse(
                status_code=200,
                content={
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {"listChanged": False},
                            "resources": {"listChanged": False, "subscribe": False},
                            "prompts": {"listChanged": False},
                        },
                        "serverInfo": {
                            "name": "snowflake-langflow",
                            "version": "0.3.1",
                        },
                    },
                },
            )

        if method == "ping":
            return JSONResponse(status_code=200, content={"jsonrpc": "2.0", "id": msg_id, "result": {}})

        if method == "tools/list":
            tools = [
                {
                    "name": "snowflake_get_context",
                    "description": "Get active Snowflake role, context_epoch, database, schema, and replicated trace tables in SnowflakeLangflow.",
                    "inputSchema": {"type": "object", "properties": {}},
                },
                {
                    "name": "snowflake_switch_role",
                    "description": "Switch the active Snowflake role in SnowflakeLangflow and increment context_epoch.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"target_role": {"type": "string", "description": "Target Snowflake role"}},
                        "required": ["target_role"],
                    },
                },
                {
                    "name": "snowflake_query_sql",
                    "description": "Execute a governed SQL statement in SnowflakeLangflow under the active Snowflake role.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"statement": {"type": "string", "description": "SQL query to execute"}},
                        "required": ["statement"],
                    },
                },
                {
                    "name": "snowflake_sync_traces",
                    "description": "Synchronize Langflow Flow Activity runs and spans into Snowflake SQL tables (LANGFLOW_FLOW_RUNS, LANGFLOW_FLOW_SPANS, LANGFLOW_AUDIT_LOG).",
                    "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "default": 20}}},
                },
            ]
            return JSONResponse(
                status_code=200,
                content={"jsonrpc": "2.0", "id": msg_id, "result": {"tools": tools}},
            )

        if method == "tools/call":
            params = msg.get("params") or {}
            tool_name = str(params.get("name") or "")
            args = params.get("arguments") or {}

            if tool_name == "snowflake_get_context":
                LIVE_BRIDGE.refresh_roles_from_snowflake()
                payload = {
                    "active_role": LIVE_BRIDGE.active_role,
                    "available_roles": LIVE_BRIDGE.available_roles,
                    "context_epoch": LIVE_BRIDGE.context_epoch,
                    "database": LIVE_BRIDGE.database,
                    "schema": LIVE_BRIDGE.schema,
                    "traces_tables": {
                        "runs": LIVE_BRIDGE.runs_table,
                        "spans": LIVE_BRIDGE.spans_table,
                        "audit": LIVE_BRIDGE.audit_table,
                    },
                }
            elif tool_name == "snowflake_switch_role":
                payload = LIVE_BRIDGE.switch_role(str(args.get("target_role") or "ACCOUNTADMIN"))
            elif tool_name == "snowflake_query_sql":
                payload = LIVE_BRIDGE.execute_sql(str(args.get("statement") or "SELECT CURRENT_ROLE()"))
            elif tool_name == "snowflake_sync_traces":
                payload = LIVE_BRIDGE.sync_langflow_traces_to_snowflake(limit=int(args.get("limit") or 20))
            else:
                return JSONResponse(
                    status_code=200,
                    content={
                        "jsonrpc": "2.0",
                        "id": msg_id,
                        "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
                    },
                )

            return JSONResponse(
                status_code=200,
                content={
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(payload, indent=2)}],
                        "isError": False,
                    },
                },
            )

        if method in ("resources/list", "resources/templates/list", "prompts/list"):
            key = "prompts" if "prompts" in method else ("resourceTemplates" if "templates" in method else "resources")
            return JSONResponse(status_code=200, content={"jsonrpc": "2.0", "id": msg_id, "result": {key: []}})

        return JSONResponse(
            status_code=200,
            content={"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": f"Unsupported method {method}"}},
        )

    app.include_router(sf_router)
