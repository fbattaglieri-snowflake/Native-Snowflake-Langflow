"""End-to-End Verification & Security/RBAC Audit Suite for SnowflakeLangflow `Triggers` (`Blocco 8`: `TRG-W2-R8-01..07`)."""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
import re
import sys
import types
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest

from lfx_snowflake.bundle_manifest import (
    INVARIANT_CATEGORIES_MANIFEST,
    SNOWFLAKE_BUNDLE_COMPONENTS,
    get_bundle_manifest,
)
from lfx_snowflake.components.data_sources import (
    filter_business_data_source_objects,
    is_technical_memory_table,
)
from lfx_snowflake.fastapi_live_bridge import (
    LiveSnowflakeBridge,
    register_snowflake_fastapi_routes,
)
from lfx_snowflake.native_category_components.triggers import (
    SnowflakeAlertTrigger,
    SnowflakeScheduleTrigger,
    SnowflakeStageFileTrigger,
    SnowflakeTableChangeTrigger,
)
from lfx_snowflake.providers.registry import ERROR_AUTHZ_ROLE_DENIED, SnowflakeProviderError
from lfx_snowflake.triggers_manager import (
    TRIGGER_EVENT_TYPES,
    SnowflakeTriggersManager,
    _quote_ident,
    build_alert_trigger_ddl,
    build_stage_file_trigger_ddl,
    build_table_change_trigger_ddl,
    sanitize_flow_short_id,
    validate_cron_expression,
    validate_file_pattern,
    validate_flow_id,
    validate_readonly_alert_sql,
    validate_timezone,
    verify_trigger_rbac_privileges,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
TRIGGERS_PKG_DIR = REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components" / "triggers"


def _make_bridge_and_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[LiveSnowflakeBridge, Any, list[str]]:
    import lfx_snowflake.fastapi_live_bridge as fb_mod
    import lfx_snowflake.triggers_manager as tm_mod

    bridge = fb_mod.LIVE_BRIDGE
    executed_sql: list[str] = []

    def _fake_exec(sql: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        executed_sql.append(sql)
        return {
            "ok": True,
            "status_code": 200,
            "mode": "mock",
            "rows": [{"ID": 1, "STATUS": "OK"}],
            "row_count": 1,
        }

    monkeypatch.setattr(bridge, "execute_sql", _fake_exec)
    monkeypatch.setattr(bridge, "active_role", "SNOWFLAKE_LANGFLOW_ADMIN_ROLE")
    monkeypatch.setattr(bridge, "database", "SNOWFLAKE_LANGFLOW")
    monkeypatch.setattr(bridge, "schema", "CORE")
    monkeypatch.setattr(bridge, "active_flow_id", None)
    monkeypatch.setattr(
        bridge,
        "available_roles",
        [
            "ACCOUNTADMIN",
            "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
            "SNOWFLAKE_LANGFLOW_DEV_ROLE",
            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
            "PUBLIC",
        ],
    )
    monkeypatch.setattr(
        tm_mod,
        "TRIGGERS_MANAGER",
        SnowflakeTriggersManager(sqlite_path=str(tmp_path / "triggers_test.db")),
    )

    class _StubJSONResponse:
        def __init__(self, status_code: int = 200, content: Any = None) -> None:
            self.status_code = int(status_code)
            self.content = content

        def json(self) -> Any:
            return self.content

    class _StubRequest:
        def __init__(
            self,
            method: str,
            url: str,
            json_body: Any = None,
            params: dict[str, str] | None = None,
        ) -> None:
            if params:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{urlencode(params)}"
            parsed = urlsplit(url)
            self.method = method.upper()
            self.url = types.SimpleNamespace(path=parsed.path)
            self.query_params = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
            self.headers: dict[str, str] = {}
            self._json_body = json_body

        async def json(self) -> Any:
            if self._json_body is None:
                raise ValueError("No JSON body")
            return self._json_body

        async def body(self) -> bytes:
            if self._json_body is None:
                return b""
            return json.dumps(self._json_body).encode("utf-8")

    class _StubAPIRouter:
        def __init__(self, prefix: str = "", tags: list[str] | None = None) -> None:
            self.prefix = prefix.rstrip("/")
            self.tags = tags or []
            self.routes: list[tuple[str, str, Any]] = []

        def _register(self, method: str, path: str):
            full_path = f"{self.prefix}{path}"

            def _decorator(fn: Any) -> Any:
                self.routes.append((method.upper(), full_path, fn))
                return fn

            return _decorator

        def get(self, path: str, **_kwargs: Any):
            return self._register("GET", path)

        def post(self, path: str, **_kwargs: Any):
            return self._register("POST", path)

        def put(self, path: str, **_kwargs: Any):
            return self._register("PUT", path)

        def patch(self, path: str, **_kwargs: Any):
            return self._register("PATCH", path)

        def delete(self, path: str, **_kwargs: Any):
            return self._register("DELETE", path)

    fastapi_mod = types.ModuleType("fastapi")
    fastapi_mod.APIRouter = _StubAPIRouter  # type: ignore[attr-defined]
    fastapi_mod.Request = _StubRequest  # type: ignore[attr-defined]
    resp_mod = types.ModuleType("fastapi.responses")
    resp_mod.JSONResponse = _StubJSONResponse  # type: ignore[attr-defined]
    sys.modules["fastapi"] = fastapi_mod
    sys.modules["fastapi.responses"] = resp_mod

    class _StubApp:
        def __init__(self) -> None:
            self.routers: list[_StubAPIRouter] = []
            self.middlewares: list[Any] = []

        def include_router(self, router: _StubAPIRouter) -> None:
            self.routers.append(router)

        def middleware(self, _kind: str = "http"):
            def _decorator(fn: Any) -> Any:
                self.middlewares.append(fn)
                return fn

            return _decorator

    app = _StubApp()
    register_snowflake_fastapi_routes(app)

    flow_router = _StubAPIRouter()

    @flow_router.put("/api/v1/flows/{flow_id}")
    async def _dummy_put_flow(flow_id: str) -> _StubJSONResponse:
        return _StubJSONResponse(status_code=200, content={"id": flow_id, "updated": True})

    @flow_router.patch("/api/v1/flows/{flow_id}")
    async def _dummy_patch_flow(flow_id: str) -> _StubJSONResponse:
        return _StubJSONResponse(status_code=200, content={"id": flow_id, "patched": True})

    @flow_router.delete("/api/v1/flows/{flow_id}")
    async def _dummy_delete_flow(flow_id: str) -> _StubJSONResponse:
        return _StubJSONResponse(status_code=200, content={"id": flow_id, "deleted": True})

    @flow_router.post("/api/v1/webhook/{flow_id}")
    async def _dummy_webhook_flow(flow_id: str) -> _StubJSONResponse:
        return _StubJSONResponse(status_code=202, content={"id": flow_id, "status": "accepted"})

    app.include_router(flow_router)

    class _FallbackTestClient:
        def _dispatch(
            self,
            method: str,
            url: str,
            json_body: Any = None,
            params: dict[str, str] | None = None,
        ) -> _StubJSONResponse:
            req = _StubRequest(method, url, json_body=json_body, params=params)
            req_path = req.url.path

            async def _call_route(r: _StubRequest) -> _StubJSONResponse:
                for router in app.routers:
                    for r_method, r_path, fn in router.routes:
                        if r_method != method.upper():
                            continue
                        pattern = "^" + re.sub(r"\{([a-zA-Z0-9_]+)(?::path)?\}", r"(?P<\1>[^/]+)", r_path) + "$"
                        m = re.match(pattern, req_path)
                        if m:
                            sig = inspect.signature(fn)
                            call_kwargs: dict[str, Any] = {}
                            for p_name in sig.parameters:
                                if p_name == "request":
                                    call_kwargs["request"] = r
                                elif p_name in m.groupdict():
                                    call_kwargs[p_name] = m.groupdict()[p_name]
                                elif p_name in r.query_params:
                                    val = r.query_params[p_name]
                                    call_kwargs[p_name] = int(val) if val.isdigit() else val
                            return await fn(**call_kwargs)
                raise AssertionError(f"Route not found: {method} {req_path}")

            async def _run_pipeline() -> _StubJSONResponse:
                if app.middlewares:
                    return await app.middlewares[0](req, _call_route)
                return await _call_route(req)

            return asyncio.run(_run_pipeline())

        def get(self, url: str, params: dict[str, str] | None = None) -> _StubJSONResponse:
            return self._dispatch("GET", url, params=params)

        def post(self, url: str, json: Any = None, params: dict[str, str] | None = None) -> _StubJSONResponse:  # noqa: A002
            return self._dispatch("POST", url, json_body=json, params=params)

        def put(self, url: str, json: Any = None, params: dict[str, str] | None = None) -> _StubJSONResponse:  # noqa: A002
            return self._dispatch("PUT", url, json_body=json, params=params)

        def patch(self, url: str, json: Any = None, params: dict[str, str] | None = None) -> _StubJSONResponse:  # noqa: A002
            return self._dispatch("PATCH", url, json_body=json, params=params)

        def delete(self, url: str, params: dict[str, str] | None = None) -> _StubJSONResponse:
            return self._dispatch("DELETE", url, params=params)

    return bridge, _FallbackTestClient(), executed_sql


# =============================================================================
# TRG-W2-R8-01: Sidebar Category `Triggers` & 1-Class-Per-File Separation
# =============================================================================


def test_trg_w2_r8_01_sidebar_category_manifest_and_1_class_per_file() -> None:
    """Verify `Triggers` category manifest, `Input & Output` invariance, and 1-class-per-file AST compliance."""
    assert INVARIANT_CATEGORIES_MANIFEST["categories"]["Input & Output"]["components"] == [
        "Chat Input",
        "Chat Output",
        "Text Input",
        "Text Output",
        "Webhook",
    ]
    assert INVARIANT_CATEGORIES_MANIFEST["categories"]["Input & Output"]["invariant"] is True

    trigger_specs = [c for c in SNOWFLAKE_BUNDLE_COMPONENTS if c.category == "Triggers"]
    assert len(trigger_specs) == 4
    expected = {
        "snowflake_schedule_trigger": "Schedule Trigger",
        "snowflake_table_change_trigger": "Table Change Trigger",
        "snowflake_stage_file_trigger": "Stage File Trigger",
        "snowflake_alert_trigger": "Condition Alert Trigger",
    }
    assert {c.component_id: c.display_name for c in trigger_specs} == expected
    for spec in trigger_specs:
        assert spec.icon == "Snowflake"
        assert spec.release_phase == "W2-R8"
        assert not spec.display_name.startswith("Snowflake ")

    manifest = get_bundle_manifest()
    manifest_triggers = [c for c in manifest["components"] if c["category"] == "Triggers"]
    assert len(manifest_triggers) == 4

    # Verify 1-class-1-file and zero module-level try/except in native_category_components/triggers/*.py
    py_files = sorted(f for f in TRIGGERS_PKG_DIR.glob("*.py") if f.name != "__init__.py")
    assert [f.name for f in py_files] == [
        "snowflake_alert_trigger.py",
        "snowflake_schedule_trigger.py",
        "snowflake_stage_file_trigger.py",
        "snowflake_table_change_trigger.py",
    ]
    for py_file in py_files:
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
        top_level_tries = [node for node in tree.body if isinstance(node, ast.Try)]
        assert not top_level_tries, f"Forbidden module-level try/except in {py_file.name}"
        class_defs = [node for node in tree.body if isinstance(node, ast.ClassDef)]
        assert len(class_defs) == 1, f"Expected strictly 1 class in {py_file.name}, found {len(class_defs)}"


def test_trg_w2_r8_01_container_hook_and_ui_patch_ordering() -> None:
    """Verify `install_live_hooks_in_container.py` and `snowflake-ui-integration.js` place `Triggers` after `Input & Output` and before `Data Sources`."""
    hook_src = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")
    assert 'idx_cats["triggers"]' in hook_src
    assert 'entries_map["input_output"]["Webhook"]' in hook_src
    assert '{display_name:"sidebar.category.triggers",name:"triggers",icon:"Zap"}' in hook_src
    assert '"sidebar.category.triggers":"Triggers"' in hook_src

    ui_src = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    assert "disclosure-triggers" in ui_src
    for badge in ("SCHEDULED_RUN", "STREAM_TRIGGER_RUN", "STAGE_FILE_TRIGGER_RUN", "ALERT_TRIGGER_RUN", "WEBHOOK_RUN"):
        assert badge in ui_src


# =============================================================================
# TRG-W2-R8-02: Service Function SQL, `/api/v1/snowflake/service-trigger`, Idempotency & Lifecycle Cleanup
# =============================================================================


def test_trg_w2_r8_02_service_function_sql_contract() -> None:
    """Verify `sql/03_triggers_service_function.sql` defines `RUN_LANGFLOW_FLOW` and role grants."""
    sql_text = (REPO_ROOT / "sql" / "03_triggers_service_function.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE FUNCTION SNOWFLAKE_LANGFLOW.CORE.RUN_LANGFLOW_FLOW(" in sql_text
    assert "SERVICE = SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_SERVICE" in sql_text
    assert "ENDPOINT = ui" in sql_text
    assert "MAX_BATCH_ROWS = 1" in sql_text
    assert "AS '/api/v1/snowflake/service-trigger'" in sql_text
    assert "SNOWFLAKE_LANGFLOW_ADMIN_ROLE" in sql_text
    assert "SNOWFLAKE_LANGFLOW_DEV_ROLE" in sql_text
    assert "SNOWFLAKE_LANGFLOW_VIEWER_ROLE" in sql_text


def test_trg_w2_r8_02_service_trigger_batch_idempotency_and_context_restoration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify `POST /api/v1/snowflake/service-trigger` batch format, idempotency, failure non-caching, and `[ ❄️ Context ]` restoration."""
    bridge, client, _ = _make_bridge_and_client(tmp_path, monkeypatch)
    flow_id = "11111111-2222-3333-4444-555555555555"
    bridge.update_selected_context(
        {
            "database": "SALES_DB",
            "schema": "ANALYTICS",
            "databases": ["SALES_DB"],
            "schemas_by_db": {"SALES_DB": ["ANALYTICS"]},
        },
        flow_id=flow_id,
    )

    batch_req = {
        "data": [
            [
                0,
                flow_id,
                {
                    "trigger_type": "SCHEDULED_RUN",
                    "scheduled_time": "2026-10-28T09:00:00Z",
                    "task_name": "LF_SCHED_111111112222",
                    "trigger_message": "Morning Sales Digest",
                },
            ]
        ]
    }
    resp1 = client.post("/api/v1/snowflake/service-trigger", json=batch_req)
    assert resp1.status_code == 200
    body1 = resp1.json()
    assert "data" in body1 and len(body1["data"]) == 1
    row_idx, res1 = body1["data"][0]
    assert row_idx == 0
    assert res1["status"] == "SUCCEEDED"
    assert res1["deduplicated"] is False
    assert res1["flow_id"] == flow_id
    assert bridge.database == "SALES_DB"
    assert bridge.schema == "ANALYTICS"

    # Re-sending identical `(flow_id, trigger_type, scheduled_time)` must deduplicate and return the same `run_id`
    resp2 = client.post("/api/v1/snowflake/service-trigger", json=batch_req)
    assert resp2.status_code == 200
    _, res2 = resp2.json()["data"][0]
    assert res2["deduplicated"] is True
    assert res2["run_id"] == res1["run_id"]

    # Failed execution must return `status="FAILED"` (so Snowflake Task/Alert raises and rolls back stream offset) and must NOT be cached
    fail_req = {
        "data": [
            [
                0,
                flow_id,
                {
                    "trigger_type": "STREAM_TRIGGER_RUN",
                    "batch_id": "batch-retry-test",
                    "simulate_failure": True,
                },
            ]
        ]
    }
    resp_fail = client.post("/api/v1/snowflake/service-trigger", json=fail_req)
    assert resp_fail.status_code == 200
    _, res_fail = resp_fail.json()["data"][0]
    assert res_fail["status"] == "FAILED"
    assert res_fail["deduplicated"] is False

    # Retry of the same batch_id without `simulate_failure` succeeds fresh
    retry_req = {
        "data": [
            [
                0,
                flow_id,
                {
                    "trigger_type": "STREAM_TRIGGER_RUN",
                    "batch_id": "batch-retry-test",
                    "simulate_failure": False,
                },
            ]
        ]
    }
    resp_retry = client.post("/api/v1/snowflake/service-trigger", json=retry_req)
    assert resp_retry.status_code == 200
    _, res_retry = resp_retry.json()["data"][0]
    assert res_retry["status"] == "SUCCEEDED"
    assert res_retry["deduplicated"] is False
    assert res_retry["run_id"] != res_fail["run_id"]


def test_trg_w2_r8_02_flow_put_and_delete_lifecycle_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify automatic provisioning and orphan cleanup on `PUT /api/v1/flows/{flow_id}` and `DELETE /api/v1/flows/{flow_id}`."""
    _, client, executed_sql = _make_bridge_and_client(tmp_path, monkeypatch)
    flow_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    flow_with_triggers = {
        "id": flow_id,
        "name": "Automated Order Monitor",
        "data": {
            "nodes": [
                {
                    "id": "sched-node-1",
                    "data": {
                        "type": "SnowflakeScheduleTrigger",
                        "node": {
                            "template": {
                                "status_mode": {"value": "Active"},
                                "schedule_type": {"value": "Custom (Cron)"},
                                "cron_expression": {"value": "0 9 * * 1-5"},
                                "timezone": {"value": "Europe/Rome"},
                                "database": {"value": "SNOWFLAKE_LANGFLOW"},
                                "schema": {"value": "CORE"},
                            }
                        },
                    },
                },
                {
                    "id": "tbl-node-2",
                    "data": {
                        "type": "SnowflakeTableChangeTrigger",
                        "node": {
                            "template": {
                                "status_mode": {"value": "Active"},
                                "database": {"value": "SNOWFLAKE_LANGFLOW"},
                                "schema": {"value": "CORE"},
                                "source_object": {"value": "CUSTOMER_ORDERS"},
                                "trigger_on": {"value": "New Rows Only (Insert)"},
                            }
                        },
                    },
                },
            ]
        },
    }

    r_put = client.put(f"/api/v1/flows/{flow_id}", json=flow_with_triggers)
    assert r_put.status_code == 200

    r_list = client.get(f"/api/v1/snowflake/triggers?flow_id={flow_id}")
    assert r_list.status_code == 200
    assert r_list.json()["count"] == 2

    # Removing `tbl-node-2` via PUT must automatically SUSPEND and DROP the removed Task & Stream
    executed_sql.clear()
    flow_only_sched = {
        "id": flow_id,
        "name": "Automated Order Monitor",
        "data": {"nodes": [flow_with_triggers["data"]["nodes"][0]]},
    }
    r_put2 = client.put(f"/api/v1/flows/{flow_id}", json=flow_only_sched)
    assert r_put2.status_code == 200
    joined_sql = "\n".join(executed_sql)
    assert "ALTER TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_TRG_AAAAAAAABBBB_CUSTOMER_ORDERS SUSPEND;" in joined_sql
    assert "DROP TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_TRG_AAAAAAAABBBB_CUSTOMER_ORDERS;" in joined_sql
    assert "DROP STREAM IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_STRM_AAAAAAAABBBB_CUSTOMER_ORDERS;" in joined_sql

    r_list2 = client.get(f"/api/v1/snowflake/triggers?flow_id={flow_id}")
    assert r_list2.json()["count"] == 1

    # Deleting the flow via `DELETE /api/v1/flows/{flow_id}` must SUSPEND and DROP all remaining triggers
    executed_sql.clear()
    r_del = client.delete(f"/api/v1/flows/{flow_id}")
    assert r_del.status_code == 200
    del_sql = "\n".join(executed_sql)
    assert "ALTER TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_AAAAAAAABBBB SUSPEND;" in del_sql
    assert "DROP TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_AAAAAAAABBBB;" in del_sql
    assert client.get(f"/api/v1/snowflake/triggers?flow_id={flow_id}").json()["count"] == 0


# =============================================================================
# TRG-W2-R8-03..05: The 4 Separate Snowflake Trigger Components
# =============================================================================


def test_trg_w2_r8_03_schedule_trigger_component(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify `SnowflakeScheduleTrigger` fields, dynamic `update_build_config`, DDL, and outputs."""
    _make_bridge_and_client(tmp_path, monkeypatch)
    comp = SnowflakeScheduleTrigger()
    comp._flow_id = "sched-flow-001"
    comp.status_mode = "Active"
    comp.schedule_type = "Custom (Cron)"
    comp.cron_expression = "0 9 * * 1-5"
    comp.timezone = "Europe/Rome"
    comp.trigger_message = "Run daily report"

    input_names = [inp.name for inp in comp.inputs]
    assert "status_mode" in input_names
    assert "schedule_type" in input_names
    assert "cron_expression" in input_names
    assert "timezone" in input_names
    assert "trigger_message" in input_names
    assert "source_object" not in input_names
    assert "stage_name" not in input_names
    assert "condition_sql" not in input_names

    cfg = {
        "schedule_type": {"value": "Custom (Cron)"},
        "cron_expression": {"show": True, "value": "0 9 * * 1-5"},
        "interval_value": {"show": False, "value": 15},
        "days_of_week": {"show": False, "value": "MON,TUE"},
        "day_of_month": {"show": False, "value": "1"},
        "at_hour": {"show": False, "value": 9},
        "at_minute": {"show": False, "value": 0},
        "compute_mode": {"value": "Serverless (Managed)"},
        "warehouse": {"show": False, "options": [], "value": "COMPUTE_WH"},
        "database": {"options": [], "value": "SNOWFLAKE_LANGFLOW"},
        "schema": {"options": [], "value": "CORE"},
    }
    comp.update_build_config(cfg, "Minutes", "schedule_type")
    assert cfg["cron_expression"]["show"] is False
    assert cfg["interval_value"]["show"] is True
    comp.update_build_config(cfg, "Warehouse", "compute_mode")
    assert cfg["warehouse"]["show"] is True

    msg = comp.build_trigger_message()
    meta = comp.build_execution_metadata()
    assert msg.text == "Run daily report"
    assert meta.data["task_fqn"] == "SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_SCHEDFLOW001"
    assert "SCHEDULE = 'USING CRON 0 9 * * 1-5 Europe/Rome'" in meta.data["ddl"]
    assert "ALTER TASK SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_SCHEDFLOW001 RESUME;" in meta.data["ddl"]

    # Switching status_mode to Paused generates SUSPEND
    comp.status_mode = "Paused"
    meta_paused = comp.build_execution_metadata()
    assert "ALTER TASK SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_SCHEDFLOW001 SUSPEND;" in meta_paused.data["ddl"]


def test_trg_w2_r8_04_table_change_trigger_component(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify `SnowflakeTableChangeTrigger` strict separation (zero Cron fields), Stream + Task DDL, and outputs."""
    _make_bridge_and_client(tmp_path, monkeypatch)
    comp = SnowflakeTableChangeTrigger()
    comp._flow_id = "stream-flow-002"
    comp.database = "SNOWFLAKE_LANGFLOW"
    comp.schema = "CORE"
    comp.source_object = "CUSTOMER_ORDERS"
    comp.trigger_on = "New Rows Only (Insert)"
    comp._trigger_rows = [{"ORDER_ID": 101, "METADATA$ACTION": "INSERT"}]

    input_names = [inp.name for inp in comp.inputs]
    assert "cron_expression" not in input_names
    assert "schedule_type" not in input_names
    assert "timezone" not in input_names
    assert "source_object" in input_names
    assert "trigger_on" in input_names

    df = comp.build_changed_rows()
    msg = comp.build_trigger_message()
    meta = comp.build_stream_metadata()
    assert len(df) == 1
    assert "detected 1 changed row(s)" in msg.text
    ddl = meta.data["ddl"]
    assert "CREATE STREAM IF NOT EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_STRM_STREAMFLOW00_CUSTOMER_ORDERS" in ddl
    assert "ON TABLE SNOWFLAKE_LANGFLOW.CORE.CUSTOMER_ORDERS" in ddl
    assert "APPEND_ONLY = TRUE" in ddl
    assert "WHEN SYSTEM$STREAM_HAS_DATA('SNOWFLAKE_LANGFLOW.CORE.LF_STRM_STREAMFLOW00_CUSTOMER_ORDERS')" in ddl
    assert "CREATE OR REPLACE TEMPORARY TABLE SNOWFLAKE_LANGFLOW.CORE._LF_STREAM_BATCH_STREAMFLOW00 AS" in ddl
    assert "RAISE STATEMENT_ERROR;" in ddl

    # Dynamic Table / View target detection
    dt_ddl = build_table_change_trigger_ddl(
        flow_id="stream-flow-002",
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        source_object="DT_DAILY_METRICS",
        object_kind="DYNAMIC TABLE",
        trigger_on="All Changes (Insert / Update / Delete)",
    )
    assert "ON DYNAMIC TABLE SNOWFLAKE_LANGFLOW.CORE.DT_DAILY_METRICS" in dt_ddl["create_stream_sql"]
    assert "APPEND_ONLY = FALSE" in dt_ddl["create_stream_sql"]


def test_trg_w2_r8_05_stage_file_and_condition_alert_triggers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify `SnowflakeStageFileTrigger` and `SnowflakeAlertTrigger` components and DDL generation."""
    _make_bridge_and_client(tmp_path, monkeypatch)

    # 1. Stage File Trigger
    stg_comp = SnowflakeStageFileTrigger()
    stg_comp._flow_id = "stage-flow-003"
    stg_comp.database = "SNOWFLAKE_LANGFLOW"
    stg_comp.schema = "CORE"
    stg_comp.stage_name = "INCOMING_DOCS_STAGE"
    stg_comp.trigger_on = "New Files Only (Upload)"
    stg_comp.file_pattern = "*.pdf"
    stg_comp._trigger_files = [{"RELATIVE_PATH": "invoice_01.pdf", "SIZE": 2048}]

    files_df = stg_comp.build_staged_files()
    stg_msg = stg_comp.build_trigger_message()
    stg_meta = stg_comp.build_file_metadata()
    assert len(files_df) == 1
    assert "1 file event(s)" in stg_msg.text
    stg_ddl = stg_meta.data["ddl"]
    assert "ALTER STAGE IF EXISTS SNOWFLAKE_LANGFLOW.CORE.INCOMING_DOCS_STAGE SET DIRECTORY = (ENABLE = TRUE);" in stg_ddl
    assert "CREATE STREAM IF NOT EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_STG_STRM_STAGEFLOW003_INCOMING_DOCS_STAGE" in stg_ddl
    assert "ON STAGE SNOWFLAKE_LANGFLOW.CORE.INCOMING_DOCS_STAGE" in stg_ddl
    assert "BUILD_SCOPED_FILE_URL(@SNOWFLAKE_LANGFLOW.CORE.INCOMING_DOCS_STAGE, RELATIVE_PATH)" in stg_ddl
    assert "RELATIVE_PATH ILIKE '%.pdf'" in stg_ddl

    # 2. Condition Alert Trigger (Periodic + Event-Driven)
    alrt_comp = SnowflakeAlertTrigger()
    alrt_comp._flow_id = "alert-flow-004"
    alrt_comp.alert_mode = "Periodic Condition Check"
    alrt_comp.check_frequency = "Every 5 Minutes"
    alrt_comp.condition_sql = (
        "SELECT ORDER_ID, TOTAL_AMOUNT FROM SNOWFLAKE_LANGFLOW.CORE.CUSTOMER_ORDERS WHERE STATUS = 'FAILED'"
    )
    alrt_comp._trigger_rows = [{"ORDER_ID": 500, "TOTAL_AMOUNT": 25000}]

    alrt_df = alrt_comp.build_alert_rows()
    alrt_msg = alrt_comp.build_trigger_message()
    alrt_meta = alrt_comp.build_alert_metadata()
    assert len(alrt_df) == 1
    assert "1 matching row(s)" in alrt_msg.text
    alrt_ddl = alrt_meta.data["ddl"]
    assert "CREATE OR ALTER ALERT SNOWFLAKE_LANGFLOW.CORE.LF_ALRT_ALERTFLOW004" in alrt_ddl
    assert "SCHEDULE = '5 MINUTE'" in alrt_ddl
    assert "SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID()" in alrt_ddl
    assert "TABLE(RESULT_SCAN(:cond_qid))" in alrt_ddl

    # Event-driven Alert mode omits SCHEDULE and enables CHANGE_TRACKING on base table
    ev_spec = build_alert_trigger_ddl(
        flow_id="alert-flow-004",
        alert_mode="Alert on New Data (Event-Driven)",
        condition_sql="SELECT * FROM SNOWFLAKE_LANGFLOW.CORE.CUSTOMER_ORDERS WHERE TOTAL_AMOUNT > 10000",
    )
    assert ev_spec["change_tracking_sql"] == (
        "ALTER TABLE IF EXISTS SNOWFLAKE_LANGFLOW.CORE.CUSTOMER_ORDERS SET CHANGE_TRACKING = TRUE;"
    )
    assert "SCHEDULE =" not in ev_spec["create_alert_sql"]


# =============================================================================
# TRG-W2-R8-06: Security Audit, Anti-SQLi, RBAC (`CURRENT_ROLE()`), & 3-Tier Observability
# =============================================================================


def test_trg_w2_r8_06_sqli_rejection_and_input_validation() -> None:
    """Verify strict rejection of SQL injection payloads across identifiers, flow_ids, Cron, Timezone, file patterns, and Alert SQL."""
    # Identifiers
    for bad_ident in ("CORE; DROP TABLE X", "CORE'--", "123_BAD", "TABLE NAME", ""):
        with pytest.raises(ValueError):
            _quote_ident(bad_ident)

    # flow_id / flow_short_id
    for bad_fid in ("flow'; DROP TASK X;--", "flow/../../etc", "flow id with spaces"):
        with pytest.raises(ValueError):
            validate_flow_id(bad_fid)
        with pytest.raises(ValueError):
            sanitize_flow_short_id(bad_fid)

    # Cron expressions (valid 5-field with L/# vs invalid/injected)
    assert validate_cron_expression("0 9 * * 1-5") == "0 9 * * 1-5"
    assert validate_cron_expression("0 18 L * ?") == "0 18 L * ?"
    assert validate_cron_expression("0 9 ? * MON#1") == "0 9 ? * MON#1"
    for bad_cron in (
        "0 9 * * 1-5; DROP TASK X",
        "0 9 * * 1-5' OR '1'='1",
        "0 9 * *",  # 4 fields
        "0 0 9 * * *",  # 6 fields
        "75 9 * * *",  # minute > 59
        "0 25 * * *",  # hour > 23
    ):
        with pytest.raises(ValueError):
            validate_cron_expression(bad_cron)

    # Timezones
    assert validate_timezone("Europe/Rome") == "Europe/Rome"
    assert validate_timezone("UTC") == "UTC"
    for bad_tz in ("Europe/Rome'; DROP TASK X;--", "Not/AZone", "UTC; SELECT 1"):
        with pytest.raises(ValueError):
            validate_timezone(bad_tz)

    # File pattern
    assert validate_file_pattern("*.pdf") == "%.pdf"
    for bad_pat in ("*.pdf'; DROP STAGE X;--", "*.pdf -- comment"):
        with pytest.raises(ValueError):
            validate_file_pattern(bad_pat)

    # Alert SQL read-only single SELECT enforcement
    assert validate_readonly_alert_sql("SELECT 1 FROM MY_TABLE WHERE X > 10;") == "SELECT 1 FROM MY_TABLE WHERE X > 10"
    assert (
        validate_readonly_alert_sql("WITH cte AS (SELECT 1 AS a) SELECT a FROM cte")
        == "WITH cte AS (SELECT 1 AS a) SELECT a FROM cte"
    )
    for bad_sql in (
        "DROP TABLE SNOWFLAKE_LANGFLOW.CORE.CUSTOMER_ORDERS",
        "SELECT 1; DROP TABLE SNOWFLAKE_LANGFLOW.CORE.CUSTOMER_ORDERS",
        "SELECT 1 FROM T -- comment injection",
        "SELECT 1 /* block comment */ FROM T",
        "SELECT $$scripting$$ FROM T",
        "INSERT INTO T VALUES (1)",
        "UPDATE T SET X = 1",
        "DELETE FROM T",
        "MERGE INTO T USING S ON 1=1 WHEN MATCHED THEN DELETE",
        "GRANT ACCOUNTADMIN TO USER X",
        "ALTER USER X SET PASSWORD = 'Y'",
        "CALL SYSTEM$WAIT(10)",
        "COPY INTO T FROM @STG",
    ):
        with pytest.raises(ValueError):
            validate_readonly_alert_sql(bad_sql)


def test_trg_w2_r8_06_rbac_enforcement_and_technical_object_filtering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify `CURRENT_ROLE()` RBAC fail-closed behavior with remediation SQL and exclusion of technical trigger objects."""
    bridge, client, _ = _make_bridge_and_client(tmp_path, monkeypatch)
    bridge.active_role = "SNOWFLAKE_LANGFLOW_VIEWER_ROLE"

    with pytest.raises(SnowflakeProviderError) as exc_info:
        verify_trigger_rbac_privileges(
            active_role=bridge.active_role,
            database="SNOWFLAKE_LANGFLOW",
            schema="CORE",
            trigger_kind="SCHEDULE",
            bridge=bridge,
        )
    assert exc_info.value.code == ERROR_AUTHZ_ROLE_DENIED
    assert exc_info.value.status_code == 403
    assert "GRANT CREATE TASK ON SCHEMA SNOWFLAKE_LANGFLOW.CORE" in exc_info.value.message
    assert "GRANT EXECUTE MANAGED TASK ON ACCOUNT" in exc_info.value.message

    # HTTP endpoint `/api/v1/snowflake/triggers/sync` returns 403 when active role is VIEWER
    resp = client.post(
        "/api/v1/snowflake/triggers/sync",
        json={
            "flow_id": "rbac-test-flow",
            "data": {
                "nodes": [
                    {
                        "id": "n1",
                        "data": {"type": "SnowflakeScheduleTrigger", "node": {"template": {}}},
                    }
                ]
            },
        },
    )
    assert resp.status_code == 403
    assert resp.json()["error_code"] == ERROR_AUTHZ_ROLE_DENIED

    # Technical trigger tables/streams/tasks are excluded from business Data Sources
    mixed = [
        "CUSTOMER_ORDERS",
        "_LF_STREAM_BATCH_123456",
        "_LF_STAGE_BATCH_123456",
        "LF_STRM_123456_CUSTOMER_ORDERS",
        "LF_STG_STRM_123456_MY_STAGE",
        "LF_SCHED_123456",
        "LF_TRG_123456_CUSTOMER_ORDERS",
        "LF_ALRT_123456",
        "LANGFLOW_TRIGGER_REGISTRY",
    ]
    assert filter_business_data_source_objects(mixed) == ["CUSTOMER_ORDERS"]
    for tech in mixed[1:]:
        assert is_technical_memory_table(tech) is True


def test_trg_w2_r8_06_three_tier_observability_all_5_event_types(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify all 5 trigger event types (`SCHEDULED_RUN`, `STREAM_TRIGGER_RUN`, `STAGE_FILE_TRIGGER_RUN`, `ALERT_TRIGGER_RUN`, `WEBHOOK_RUN`) record 3-tier telemetry."""
    bridge, client, executed_sql = _make_bridge_and_client(tmp_path, monkeypatch)
    flow_id = "obs-flow-12345678"

    for idx, ev_type in enumerate(("SCHEDULED_RUN", "STREAM_TRIGGER_RUN", "STAGE_FILE_TRIGGER_RUN", "ALERT_TRIGGER_RUN")):
        r = client.post(
            "/api/v1/snowflake/service-trigger",
            json={
                "data": [
                    [
                        idx,
                        flow_id,
                        {
                            "trigger_type": ev_type,
                            "batch_id": f"obs-slot-{idx}",
                        },
                    ]
                ]
            },
        )
        assert r.status_code == 200
        assert r.json()["data"][0][1]["trigger_type"] == ev_type

    # 5th trigger event type: native Webhook (`/api/v1/webhook/{flow_id}`)
    r_wh = client.post(f"/api/v1/webhook/{flow_id}", json={"event": "order_created"})
    assert r_wh.status_code == 202

    recorded_types = {
        ev["event_type"]
        for ev in bridge._recent_activity_events
        if ev.get("flow_id") == flow_id
    }
    assert set(TRIGGER_EVENT_TYPES).issubset(recorded_types)

    all_sql = "\n".join(executed_sql)
    assert "MERGE INTO SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_FLOW_ACTIVITY" in all_sql
    assert "MERGE INTO SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_FLOW_RUNS" in all_sql
    assert "MERGE INTO SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_FLOW_SPANS" in all_sql
    assert "INSERT INTO SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_AUDIT_LOG" in all_sql


def test_trg_w2_r8_verifier_schedule_modes_and_delete_triggers_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify Schedule Trigger interval/warehouse modes, Stage/Alert dynamic build config, and `DELETE /api/v1/snowflake/triggers/{flow_id}`."""
    _, client, executed_sql = _make_bridge_and_client(tmp_path, monkeypatch)
    flow_id = "verifier-flow-009"

    # 1. Schedule Trigger interval modes (Minutes, Hours, Days, Weeks, Months) + Warehouse compute mode
    comp = SnowflakeScheduleTrigger()
    comp._flow_id = flow_id
    comp.compute_mode = "Warehouse"
    comp.warehouse = "COMPUTE_WH"

    for stype, expected_sched in (
        ("Minutes", "SCHEDULE = '15 MINUTE'"),
        ("Hours", "SCHEDULE = '60 MINUTE'"),
        ("Days", "SCHEDULE = 'USING CRON 0 9 * * * Europe/Rome'"),
        ("Weeks", "SCHEDULE = 'USING CRON 0 9 * * MON,TUE,WED,THU,FRI Europe/Rome'"),
        ("Months", "SCHEDULE = 'USING CRON 0 9 1 * * Europe/Rome'"),
    ):
        comp.schedule_type = stype
        comp.interval_value = 15 if stype == "Minutes" else 1
        meta = comp.build_execution_metadata()
        assert expected_sched in meta.data["ddl"]
        assert "WAREHOUSE = COMPUTE_WH" in meta.data["ddl"]

    # 2. Stage File Trigger `update_build_config` and event modes
    stg_comp = SnowflakeStageFileTrigger()
    stg_cfg = {
        "database": {"options": [], "value": "SNOWFLAKE_LANGFLOW"},
        "schema": {"options": [], "value": "CORE"},
        "stage_name": {"options": [], "value": "SNOWFLAKE_LANGFLOW_STAGE"},
        "compute_mode": {"value": "Serverless (Managed)"},
        "warehouse": {"show": False, "options": [], "value": "COMPUTE_WH"},
    }
    stg_comp.update_build_config(stg_cfg, "Warehouse", "compute_mode")
    assert stg_cfg["warehouse"]["show"] is True

    stg_upd_ddl = build_stage_file_trigger_ddl(
        flow_id=flow_id,
        trigger_on="New & Updated Files",
        file_pattern=None,
    )
    assert "WHERE METADATA$ACTION = 'INSERT'" in stg_upd_ddl["create_task_sql"]

    stg_all_ddl = build_stage_file_trigger_ddl(
        flow_id=flow_id,
        trigger_on="All File Events (Upload / Update / Delete)",
        file_pattern=None,
    )
    assert "METADATA$ACTION = 'INSERT'" not in stg_all_ddl["create_task_sql"]

    # 3. Condition Alert Trigger `update_build_config` mode toggling
    alrt_comp = SnowflakeAlertTrigger()
    alrt_cfg = {
        "database": {"options": [], "value": "SNOWFLAKE_LANGFLOW"},
        "schema": {"options": [], "value": "CORE"},
        "alert_mode": {"value": "Periodic Condition Check"},
        "check_frequency": {"show": True, "value": "Every 5 Minutes"},
        "cron_expression": {"show": False, "value": "*/5 * * * *"},
        "timezone": {"show": False, "value": "Europe/Rome"},
        "compute_mode": {"value": "Serverless (Managed)"},
        "warehouse": {"show": False, "options": [], "value": "COMPUTE_WH"},
    }
    alrt_comp.update_build_config(alrt_cfg, "Custom (Cron)", "check_frequency")
    assert alrt_cfg["cron_expression"]["show"] is True
    assert alrt_cfg["timezone"]["show"] is True
    alrt_comp.update_build_config(alrt_cfg, "Alert on New Data (Event-Driven)", "alert_mode")
    assert alrt_cfg["check_frequency"]["show"] is False
    assert alrt_cfg["cron_expression"]["show"] is False
    assert alrt_cfg["timezone"]["show"] is False

    # 4. Direct `DELETE /api/v1/snowflake/triggers/{flow_id}` cleanup endpoint
    executed_sql.clear()
    r_del = client.delete(f"/api/v1/snowflake/triggers/{flow_id}")
    assert r_del.status_code == 200
    assert r_del.json()["ok"] is True
    assert len(r_del.json()["statements"]) >= 2
    assert "DROP TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_VERIFIERFLOW;" in "\n".join(executed_sql)


# =============================================================================
# TRG-W2-R8-07: Real-Time Status Toggle, Target Replacement & `[ ❄️ Context ]` Adaptation
# =============================================================================


def test_trg_w2_r8_07_realtime_status_toggle_and_target_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify instant `Status` (`Active` <-> `Paused`), schedule `ALTER_IN_PLACE`, and target object replacement (`DROP` old Task/Stream)."""
    _, client, executed_sql = _make_bridge_and_client(tmp_path, monkeypatch)
    flow_id = "rt-sync-flow-007"

    # 1. Provision Schedule Trigger via `update_build_config` with `status_mode = "Active"`
    sched = SnowflakeScheduleTrigger()
    sched._flow_id = flow_id
    sched._id = "sched-node-rt"
    sched_cfg = {
        "status_mode": {"value": "Active"},
        "schedule_type": {"value": "Custom (Cron)"},
        "cron_expression": {"show": True, "value": "0 9 * * 1-5"},
        "timezone": {"value": "Europe/Rome"},
        "database": {"options": [], "value": "SNOWFLAKE_LANGFLOW"},
        "schema": {"options": [], "value": "CORE"},
        "compute_mode": {"value": "Serverless (Managed)"},
        "warehouse": {"show": False, "options": [], "value": "COMPUTE_WH"},
    }
    executed_sql.clear()
    sched.update_build_config(sched_cfg, "Active", "status_mode")
    assert sched_cfg["_last_trigger_sync"]["transition_mode"] == "CREATE"
    assert "RESUMED" in sched_cfg["status_mode"]["info"]
    assert any("CREATE OR ALTER TASK SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_RTSYNCFLOW00" in s for s in executed_sql)
    assert any("ALTER TASK SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_RTSYNCFLOW00 RESUME;" in s for s in executed_sql)

    # 2. Clicking `Paused` on `status_mode` executes ONLY `ALTER TASK ... SUSPEND;` (`STATUS_TOGGLE_ONLY`), preserving TASK_HISTORY
    executed_sql.clear()
    sched.update_build_config(sched_cfg, "Paused", "status_mode")
    assert sched_cfg["_last_trigger_sync"]["transition_mode"] == "STATUS_TOGGLE_ONLY"
    assert "SUSPENDED" in sched_cfg["status_mode"]["info"]
    assert "ALTER TASK SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_RTSYNCFLOW00 SUSPEND;" in executed_sql
    assert not any("CREATE OR ALTER TASK" in s for s in executed_sql)

    # 3. Direct REST endpoint `POST /api/v1/snowflake/triggers/status` toggles back to `Active` (`RESUME`)
    executed_sql.clear()
    r_status = client.post(
        "/api/v1/snowflake/triggers/status",
        json={"flow_id": flow_id, "node_id": "sched-node-rt", "status_mode": "Active"},
    )
    assert r_status.status_code == 200
    assert r_status.json()["status_mode"] == "Active"
    assert r_status.json()["statement"] == "ALTER TASK SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_RTSYNCFLOW00 RESUME;"
    assert "ALTER TASK SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_RTSYNCFLOW00 RESUME;" in executed_sql

    # 4. Modifying `cron_expression` on the existing Schedule Trigger runs `SUSPEND -> ALTER TASK ... SET SCHEDULE -> CREATE OR ALTER -> RESUME`
    executed_sql.clear()
    sched_cfg["status_mode"]["value"] = "Active"
    sched.update_build_config(sched_cfg, "0 18 * * 1-5", "cron_expression")
    assert sched_cfg["_last_trigger_sync"]["transition_mode"] == "ALTER_IN_PLACE"
    joined_alter = "\n".join(executed_sql)
    assert "ALTER TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_RTSYNCFLOW00 SUSPEND;" in joined_alter
    assert "ALTER TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_RTSYNCFLOW00 SET SCHEDULE = 'USING CRON 0 18 * * 1-5 Europe/Rome';" in joined_alter
    assert "ALTER TASK SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_RTSYNCFLOW00 RESUME;" in joined_alter

    # 5. Changing `source_object` on an existing `SnowflakeTableChangeTrigger` immediately drops the old Task & Stream and creates the new ones
    tbl = SnowflakeTableChangeTrigger()
    tbl._flow_id = flow_id
    tbl._id = "tbl-node-rt"
    tbl_cfg = {
        "status_mode": {"value": "Active"},
        "database": {"options": [], "value": "SNOWFLAKE_LANGFLOW"},
        "schema": {"options": [], "value": "CORE"},
        "source_object": {"options": [], "value": "CUSTOMER_ORDERS"},
        "trigger_on": {"value": "New Rows Only (Insert)"},
        "compute_mode": {"value": "Serverless (Managed)"},
        "warehouse": {"show": False, "options": [], "value": "COMPUTE_WH"},
    }
    tbl.update_build_config(tbl_cfg, "Active", "status_mode")
    assert tbl_cfg["_last_trigger_sync"]["transition_mode"] == "CREATE"

    executed_sql.clear()
    tbl.update_build_config(tbl_cfg, "INVOICE_EVENTS", "source_object")
    assert tbl_cfg["_last_trigger_sync"]["transition_mode"] == "REPLACE_TARGET_OBJECT"
    rep_sql = "\n".join(executed_sql)
    assert "ALTER TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_TRG_RTSYNCFLOW00_CUSTOMER_ORDERS SUSPEND;" in rep_sql
    assert "DROP TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_TRG_RTSYNCFLOW00_CUSTOMER_ORDERS;" in rep_sql
    assert "DROP STREAM IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_STRM_RTSYNCFLOW00_CUSTOMER_ORDERS;" in rep_sql
    assert "CREATE STREAM IF NOT EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_STRM_RTSYNCFLOW00_INVOICE_EVENTS" in rep_sql
    assert "CREATE OR ALTER TASK SNOWFLAKE_LANGFLOW.CORE.LF_TRG_RTSYNCFLOW00_INVOICE_EVENTS" in rep_sql


def test_trg_w2_r8_07_context_change_and_role_switch_auto_pause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify reactive `[ ❄️ Context ]` update and role switch automatically suspend out-of-scope or unauthorized active triggers."""
    bridge, client, executed_sql = _make_bridge_and_client(tmp_path, monkeypatch)
    flow_id = "ctx-adapt-flow-008"

    bridge.update_selected_context(
        {
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "databases": ["SNOWFLAKE_LANGFLOW"],
            "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE"]},
            "objects_by_db_schema": {
                "SNOWFLAKE_LANGFLOW.CORE": {
                    "tables": ["CUSTOMER_ORDERS", "INVOICE_EVENTS"],
                    "stages": ["SNOWFLAKE_LANGFLOW_STAGE"],
                }
            },
        },
        flow_id=flow_id,
    )

    # Provision a Table Change Trigger on `CUSTOMER_ORDERS` and a Schedule Trigger on `SNOWFLAKE_LANGFLOW.CORE`
    tbl = SnowflakeTableChangeTrigger()
    tbl._flow_id = flow_id
    tbl._id = "tbl-ctx-1"
    tbl_cfg = {
        "status_mode": {"value": "Active"},
        "database": {"options": [], "value": "SNOWFLAKE_LANGFLOW"},
        "schema": {"options": [], "value": "CORE"},
        "source_object": {"options": [], "value": "CUSTOMER_ORDERS"},
        "trigger_on": {"value": "New Rows Only (Insert)"},
        "compute_mode": {"value": "Serverless (Managed)"},
        "warehouse": {"show": False, "options": [], "value": "COMPUTE_WH"},
    }
    tbl.update_build_config(tbl_cfg, "Active", "status_mode")

    sched = SnowflakeScheduleTrigger()
    sched._flow_id = flow_id
    sched._id = "sched-ctx-1"
    sched_cfg = {
        "status_mode": {"value": "Active"},
        "schedule_type": {"value": "Minutes"},
        "interval_value": {"value": 15},
        "database": {"options": [], "value": "SNOWFLAKE_LANGFLOW"},
        "schema": {"options": [], "value": "CORE"},
        "compute_mode": {"value": "Serverless (Managed)"},
        "warehouse": {"show": False, "options": [], "value": "COMPUTE_WH"},
    }
    sched.update_build_config(sched_cfg, "Active", "status_mode")

    # 1. Removing `CUSTOMER_ORDERS` from `[ ❄️ Context ]` must auto-pause `tbl-ctx-1` (`ALTER TASK ... SUSPEND;`) while keeping `sched-ctx-1` Active
    executed_sql.clear()
    ctx_res = bridge.update_selected_context(
        {
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "databases": ["SNOWFLAKE_LANGFLOW"],
            "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE"]},
            "objects_by_db_schema": {
                "SNOWFLAKE_LANGFLOW.CORE": {
                    "tables": ["INVOICE_EVENTS"],
                    "stages": ["SNOWFLAKE_LANGFLOW_STAGE"],
                }
            },
        },
        flow_id=flow_id,
    )
    auto_paused = ctx_res["reconciled_triggers"]["auto_paused"]
    assert len(auto_paused) == 1
    assert auto_paused[0]["trigger_id"] == f"{flow_id}::tbl-ctx-1"
    assert "ALTER TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_TRG_CTXADAPTFLOW_CUSTOMER_ORDERS SUSPEND;" in executed_sql

    triggers_after_ctx = {t["node_id"]: t["status_mode"] for t in bridge.list_flow_triggers(flow_id=flow_id)}
    assert triggers_after_ctx["tbl-ctx-1"] == "Paused"
    assert triggers_after_ctx["sched-ctx-1"] == "Active"

    # 2. Switching to read-only `SNOWFLAKE_LANGFLOW_VIEWER_ROLE` must auto-pause the remaining active trigger `sched-ctx-1`
    executed_sql.clear()
    role_res = bridge.switch_role("SNOWFLAKE_LANGFLOW_VIEWER_ROLE", flow_id=flow_id)
    assert role_res["ok"] is True
    role_paused = role_res["reconciled_triggers"]["auto_paused"]
    assert len(role_paused) == 1
    assert role_paused[0]["trigger_id"] == f"{flow_id}::sched-ctx-1"
    assert "ALTER TASK IF EXISTS SNOWFLAKE_LANGFLOW.CORE.LF_SCHED_CTXADAPTFLOW SUSPEND;" in executed_sql

    triggers_after_role = {t["node_id"]: t["status_mode"] for t in bridge.list_flow_triggers(flow_id=flow_id)}
    assert triggers_after_role["sched-ctx-1"] == "Paused"


