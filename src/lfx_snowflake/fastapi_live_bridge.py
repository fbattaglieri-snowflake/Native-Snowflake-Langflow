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
        self._roles_discovered: bool = False
        self._superuser_synced: bool = False

    def sync_superuser_credentials(self) -> None:
        """Ensure the SQLite `user` table superuser matches `LANGFLOW_SUPERUSER` and `LANGFLOW_SUPERUSER_PASSWORD`."""
        if self._superuser_synced:
            return
        su_user = os.getenv("LANGFLOW_SUPERUSER", "").strip()
        su_pass = os.getenv("LANGFLOW_SUPERUSER_PASSWORD", "").strip()
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        db_path = persist_dir / "langflow.db"
        if not su_user or not su_pass or not db_path.exists():
            return
        try:
            from langflow.services.auth.utils import get_password_hash

            hashed = get_password_hash(su_pass)
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute(
                "UPDATE user SET username = ?, password = ?, is_active = 1, is_superuser = 1 WHERE is_superuser = 1",
                (su_user, hashed),
            )
            conn.commit()
            conn.close()
            self._superuser_synced = True
        except Exception:  # noqa: BLE001
            pass

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
            "cache_invalidated": True,
        }

    def check_write_privilege_or_deny(self, action: str, target_object: str) -> dict[str, Any] | None:
        """Fail-closed role check: VIEWER and PUBLIC roles are explicitly denied DDL/write actions."""
        if self.active_role.upper() in ("PUBLIC", "SNOWFLAKE_LANGFLOW_VIEWER_ROLE", "VIEWER_ROLE"):
            msg = f"Il ruolo attivo ({self.active_role}) non consente questa operazione ({action} su {target_object})."
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
                "authenticated_user": LIVE_BRIDGE.authenticated_user,
                "active_role": LIVE_BRIDGE.active_role,
                "available_roles": LIVE_BRIDGE.available_roles,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "secondary_roles_mode": LIVE_BRIDGE.secondary_roles_mode,
                "database": LIVE_BRIDGE.database,
                "schema": LIVE_BRIDGE.schema,
                "stage_name": f"@{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.{LIVE_BRIDGE.stage_name}",
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

    @sf_router.post("/role")
    async def post_switch_role(request: Request) -> JSONResponse:
        body = await request.json()
        target_role = str(body.get("target_role") or body.get("role") or "")
        result = LIVE_BRIDGE.switch_role(target_role)
        return JSONResponse(status_code=result.get("status_code", 200), content=result)

    @sf_router.get("/catalog")
    async def get_snowflake_catalog(database: str | None = None, schema: str | None = None) -> JSONResponse:
        db = (database or LIVE_BRIDGE.database).strip()
        sch = (schema or LIVE_BRIDGE.schema).strip()
        tables_res = LIVE_BRIDGE.execute_sql(f"SHOW OBJECTS IN SCHEMA {db}.{sch}")
        stages_res = LIVE_BRIDGE.execute_sql(f"SHOW STAGES IN SCHEMA {db}.{sch}")
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "active_role": LIVE_BRIDGE.active_role,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "database": db,
                "schema": sch,
                "objects": tables_res.get("rows", []),
                "stages": stages_res.get("rows", []),
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
        obj_kind = str(body.get("object_kind") or "table").lower()
        dry_run = bool(body.get("dry_run", False))
        confirmed = bool(body.get("confirmed", False))
        target_name = str(body.get("name") or body.get("object_name") or "NEW_OBJECT").strip()

        denied = LIVE_BRIDGE.check_write_privilege_or_deny(f"PROVISION_{obj_kind.upper()}", target_name)
        if denied:
            return JSONResponse(status_code=403, content=denied)

        db = str(body.get("database") or LIVE_BRIDGE.database).strip()
        sch = str(body.get("schema") or LIVE_BRIDGE.schema).strip()

        if obj_kind == "database":
            ddl = f"CREATE DATABASE IF NOT EXISTS {target_name};"
        elif obj_kind == "schema":
            ddl = f"CREATE SCHEMA IF NOT EXISTS {db}.{target_name};"
        elif obj_kind == "stage":
            ddl = (
                f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{target_name} "
                f"DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
            )
        elif obj_kind == "postgres":
            family = str(body.get("compute_family") or "STANDARD_M")
            storage_gb = int(body.get("storage_size_gb") or 10)
            ddl = (
                f"CREATE POSTGRES INSTANCE IF NOT EXISTS {target_name} "
                f"COMPUTE_FAMILY = '{family}' STORAGE_SIZE_GB = {storage_gb} "
                f"AUTHENTICATION_AUTHORITY = POSTGRES_OR_SNOWFLAKE;"
            )
        else:
            table_type = str(body.get("table_type") or "STANDARD").upper()
            vector_dim = int(body.get("vector_dimension") or 768)
            if table_type == "HYBRID":
                ddl = (
                    f"CREATE HYBRID TABLE IF NOT EXISTS {db}.{sch}.{target_name} ("
                    f"id VARCHAR(128) PRIMARY KEY, content VARCHAR, metadata VARIANT, "
                    f"embedding VECTOR(FLOAT, {vector_dim}), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
                )
            else:
                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {db}.{sch}.{target_name} ("
                    f"id VARCHAR(128) PRIMARY KEY, content VARCHAR, metadata VARIANT, "
                    f"embedding VECTOR(FLOAT, {vector_dim}), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
                )

        if dry_run or not confirmed:
            return JSONResponse(
                status_code=200,
                content={
                    "ok": True,
                    "dry_run": True,
                    "confirmed": confirmed,
                    "active_role": LIVE_BRIDGE.active_role,
                    "ddl_preview": ddl,
                    "message": "Dry-run DDL preview generated. Set confirmed=true to execute on Snowflake.",
                },
            )

        exec_res = LIVE_BRIDGE.execute_sql(ddl)
        LIVE_BRIDGE.record_audit_event(
            action=f"PROVISION_{obj_kind.upper()}",
            outcome="ALLOWED" if exec_res.get("ok") else "ERROR",
            error_code=exec_res.get("error_code"),
            target_object=f"{db}.{sch}.{target_name}",
            details={"ddl": ddl, "result": exec_res},
        )
        return JSONResponse(
            status_code=exec_res.get("status_code", 200 if exec_res.get("ok") else 500),
            content={"ok": exec_res.get("ok"), "ddl_executed": ddl, "result": exec_res},
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
                "supported_backends": [
                    "Standard Table (VECTOR)",
                    "Hybrid Table (VECTOR)",
                    "Cortex Search",
                    "Snowflake Postgres (pgvector)",
                ],
                "supported_embeddings": [
                    "snowflake-arctic-embed-l-v2.0 (1024d)",
                    "snowflake-arctic-embed-m-v1.5 (768d)",
                    "e5-base-v2 (768d)",
                    "ollama/nomic-embed-text (768d)",
                ],
            },
        )

    @sf_router.post("/memories")
    async def post_create_memory(request: Request) -> JSONResponse:
        body = await request.json()
        name = str(body.get("name") or "").strip()
        backend = str(body.get("vector_database") or "Standard Table (VECTOR)")
        embedding_model = str(body.get("embedding_model") or "snowflake-arctic-embed-m-v1.5")
        batch_size = int(body.get("batch_size") or 64)
        llm_preprocessing = body.get("llm_preprocessing") or None

        denied = LIVE_BRIDGE.check_write_privilege_or_deny("CREATE_MEMORY", name or "MEMORY_TABLE")
        if denied:
            return JSONResponse(status_code=403, content=denied)

        safe_table = "".join(c if c.isalnum() or c == "_" else "_" for c in (name or "MEMORY_STORE")).upper()
        dim = 1024 if "1024" in embedding_model or "l-v2" in embedding_model else 768
        table_kw = "HYBRID TABLE" if "Hybrid" in backend else "TABLE"
        ddl = (
            f"CREATE {table_kw} IF NOT EXISTS {LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.MEM_{safe_table} ("
            f"chunk_id VARCHAR(128) PRIMARY KEY, memory_name VARCHAR(256), text_chunk VARCHAR, "
            f"embedding VECTOR(FLOAT, {dim}), metadata VARIANT, created_by_role VARCHAR(128), "
            f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
        )
        exec_res = LIVE_BRIDGE.execute_sql(ddl)
        entry = {
            "name": name,
            "table_fqn": f"{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.MEM_{safe_table}",
            "vector_database": backend,
            "embedding_model": embedding_model,
            "dimension": dim,
            "batch_size": batch_size,
            "llm_preprocessing": llm_preprocessing,
            "created_by_role": LIVE_BRIDGE.active_role,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "snowflake_created": bool(exec_res.get("ok")),
        }
        LIVE_BRIDGE.memories_registry.append(entry)
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
