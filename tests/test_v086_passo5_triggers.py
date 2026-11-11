"""v0.8.6 PASSO 5 regressions: BUG-TRG-01..06 (Snowflake trigger DDL + `/service-trigger` native runs)."""

from __future__ import annotations

import re

import pytest

from lfx_snowflake import triggers_manager as tm
from lfx_snowflake.triggers_manager import (
    TRIGGERS_MANAGER,
    build_alert_trigger_ddl,
    build_schedule_clause,
    build_schedule_trigger_ddl,
    build_stage_file_trigger_ddl,
    build_table_change_trigger_ddl,
    validate_cron_expression,
)

FLOW = "11111111-2222-3333-4444-555555555555"


def _all_ddl() -> dict[str, str]:
    return {
        "schedule": build_schedule_trigger_ddl(flow_id=FLOW)["create_task_sql"],
        "table": build_table_change_trigger_ddl(flow_id=FLOW, source_object="DB.SCH.ORDERS")["create_task_sql"],
        "stage": build_stage_file_trigger_ddl(flow_id=FLOW, stage_name="DB.SCH.DOCS")["create_task_sql"],
        "alert": build_alert_trigger_ddl(flow_id=FLOW, condition_sql="SELECT 1 FROM DB.SCH.T")["create_alert_sql"],
    }


# --------------------------------------------------------------------------- BUG-TRG-01 / 02


@pytest.mark.parametrize("kind", ["schedule", "table", "stage", "alert"])
def test_trg01_custom_exception_does_not_shadow_builtin_statement_error(kind) -> None:
    ddl = _all_ddl()[kind]
    assert "STATEMENT_ERROR" not in ddl
    assert "LF_FLOW_RUN_FAILED EXCEPTION (-2000" in ddl
    assert "RAISE LF_FLOW_RUN_FAILED;" in ddl


@pytest.mark.parametrize("kind", ["schedule", "table", "stage"])
def test_trg02_set_return_value_uses_a_variable(kind) -> None:
    ddl = _all_ddl()[kind]
    assert "SYSTEM$SET_RETURN_VALUE(TO_VARCHAR" not in ddl
    assert re.search(r"^\s*rv VARCHAR;$", ddl, re.M)
    assert "rv := TO_VARCHAR(res);" in ddl
    assert "CALL SYSTEM$SET_RETURN_VALUE(:rv);" in ddl


# --------------------------------------------------------------------------- BUG-TRG-03


@pytest.mark.parametrize("kind", ["table", "stage"])
def test_trg03_serverless_triggered_task_sets_target_completion_interval(kind) -> None:
    ddl = _all_ddl()[kind]
    assert "WHEN SYSTEM$STREAM_HAS_DATA(" in ddl
    assert "WAREHOUSE =" not in ddl
    assert "TARGET_COMPLETION_INTERVAL = '15 MINUTES'" in ddl


def test_trg03_interval_configurable_validated_and_warehouse_mode_unchanged() -> None:
    ddl = build_table_change_trigger_ddl(
        flow_id=FLOW, source_object="DB.SCH.ORDERS", target_completion_interval="2 hours"
    )["create_task_sql"]
    assert "TARGET_COMPLETION_INTERVAL = '2 HOURS'" in ddl
    wh = build_stage_file_trigger_ddl(
        flow_id=FLOW, stage_name="DB.SCH.DOCS", compute_mode="Warehouse", warehouse="WH1"
    )["create_task_sql"]
    assert "WAREHOUSE = WH1" in wh and "TARGET_COMPLETION_INTERVAL" not in wh
    # scheduled (non-triggered) tasks never get it
    assert "TARGET_COMPLETION_INTERVAL" not in _all_ddl()["schedule"]
    for bad in ("15 MINUTES'; DROP TABLE X; --", "0 MINUTES", "forever"):
        with pytest.raises(ValueError):
            build_table_change_trigger_ddl(flow_id=FLOW, source_object="DB.SCH.ORDERS", target_completion_interval=bad)


# --------------------------------------------------------------------------- BUG-TRG-05


@pytest.mark.parametrize("expr", ["*/5 * * * *", "0 */2 * * *", "0,30 9-17/2 * * MON-FRI", "*/15 * */3 * *"])
def test_trg05_cron_steps_accepted(expr) -> None:
    assert validate_cron_expression(expr) == expr


def test_trg05_alert_default_cron_is_buildable() -> None:
    ddl = build_alert_trigger_ddl(
        flow_id=FLOW, condition_sql="SELECT 1 FROM DB.SCH.T", check_frequency="Custom (Cron)", cron_expression="*/5 * * * *"
    )["create_alert_sql"]
    assert "SCHEDULE = 'USING CRON */5 * * * * " in ddl


@pytest.mark.parametrize("expr", ["*/5 * * * *'", "*/5 * * * *; DROP", "-- x", "*/5 * * * * $$", "*/5 \\ * * *", "*/99 * * * *"])
def test_trg05_cron_injection_still_rejected(expr) -> None:
    with pytest.raises(ValueError):
        validate_cron_expression(expr)


# --------------------------------------------------------------------------- BUG-TRG-06


@pytest.mark.parametrize("stype", ["Hourly", "minutes", "Seconds", "Cron"])
def test_trg06_unknown_schedule_type_raises(stype) -> None:
    with pytest.raises(ValueError, match="Unknown schedule_type"):
        build_schedule_clause(schedule_type=stype)


@pytest.mark.parametrize("stype, value", [("Minutes", 0), ("Minutes", -1), ("Minutes", 11521), ("Hours", 0), ("Hours", None), ("Hours", "x")])
def test_trg06_out_of_range_interval_raises(stype, value) -> None:
    with pytest.raises(ValueError):
        build_schedule_clause(schedule_type=stype, interval_value=value)


def test_trg06_valid_schedules_unchanged() -> None:
    assert build_schedule_clause(schedule_type="Minutes", interval_value=5) == "5 MINUTE"
    assert build_schedule_clause(schedule_type="Hours", interval_value=2) == "120 MINUTE"
    assert build_schedule_clause(schedule_type="Custom (Cron)", cron_expression="*/5 * * * *", timezone_name="UTC") == (
        "USING CRON */5 * * * * UTC"
    )


# --------------------------------------------------------------------------- BUG-TRG-04


class _Bridge:
    """Minimal bridge: per-flow role/context, telemetry sink."""

    def __init__(self, role: str | None = "DEV_ROLE", ctx: dict | None = None) -> None:
        self._role = role
        self._ctx = ctx if ctx is not None else {"database": "SALES_DB", "warehouse": "FLOW_WH"}
        self.telemetry: list[dict] = []
        self.warehouse = None

    @property
    def active_role(self):
        return self._role

    def get_flow_context(self, flow_id, *, create=True):
        return {**self._ctx, "flow_id": flow_id, "active_role": self._role}

    def record_trigger_execution_telemetry(self, **kw):
        self.telemetry.append(kw)


@pytest.fixture()
def runner(monkeypatch):
    calls: list[dict] = []
    state = {"result": None, "exc": None}

    async def _run(*, flow_id, trigger_payload, run_id):
        from lfx_snowflake import flow_context_guard as g
        from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID

        calls.append(
            {"flow_id": flow_id, "payload": trigger_payload, "run_id": run_id,
             "ctx_fid": CURRENT_SNOWFLAKE_FLOW_ID.get(), "component_scope": g.FLOW_COMPONENT_SQL.get()}
        )
        if state["exc"]:
            raise state["exc"]
        return state["result"] or {"found": True, "run_id": run_id, "session_id": "sess-1", "output_text": "hello"}

    monkeypatch.setattr(TRIGGERS_MANAGER, "native_flow_runner", _run)
    monkeypatch.setattr(TRIGGERS_MANAGER, "_idempotency_ledger", {})
    monkeypatch.setattr(TRIGGERS_MANAGER, "_triggers_by_id", {})
    _register(FLOW, "DEV_ROLE")
    return calls, state


def _register(fid: str, role: str, task: str = "LF_SCHED_X", updated_at: str = "2026-11-11T00:00:00") -> None:
    rec = tm.RegisteredTriggerRecord(
        trigger_id=f"t-{fid}-{task}", flow_id=fid, flow_short_id=fid[:12], node_id=task,
        trigger_component="SnowflakeScheduleTrigger", trigger_type="SCHEDULED_RUN", status_mode="Active",
        database="DB", schema="SCH", primary_object_fqn=f"DB.SCH.{task}", active_role=role, updated_at=updated_at,
    )
    TRIGGERS_MANAGER._triggers_by_id[rec.trigger_id] = rec


def _invoke(payload: dict, bridge=None, fid: str = FLOW) -> dict:
    return TRIGGERS_MANAGER.execute_service_trigger_batch(
        {"data": [[0, fid, payload]]}, bridge=bridge if bridge is not None else _Bridge()
    )["data"][0][1]


def test_trg04_really_runs_flow_in_its_scope_and_propagates_result(runner) -> None:
    calls, _ = runner
    bridge = _Bridge()
    res = _invoke({"trigger_type": "SCHEDULED_RUN", "trigger_message": "go"}, bridge)
    assert res["status"] == "SUCCEEDED" and res["error_code"] is None
    assert len(calls) == 1 and calls[0]["flow_id"] == FLOW and calls[0]["payload"]["trigger_message"] == "go"
    assert calls[0]["ctx_fid"] == FLOW  # run_in_flow_scope set the flow id for the whole native run
    assert calls[0]["component_scope"] is False  # only component wrappers mark SQL for context checks
    assert res["run_id"] == calls[0]["run_id"] and res["session_id"] == "sess-1" and res["output_text"] == "hello"
    assert res["active_role"] == "DEV_ROLE" and res["warehouse"] == "FLOW_WH"  # persisted trigger role
    assert isinstance(res["duration_ms"], float)
    assert bridge.telemetry[0]["status"] == "SUCCEEDED" and bridge.telemetry[0]["run_id"] == res["run_id"]
    from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID

    assert CURRENT_SNOWFLAKE_FLOW_ID.get() is None


def test_trg04_unknown_flow_fails_with_404_and_is_not_cached(runner) -> None:
    calls, state = runner
    state["result"] = {"found": False}
    p = {"trigger_type": "SCHEDULED_RUN", "scheduled_time": "2026-11-11T09:00:00Z"}
    res = _invoke(p)
    assert res["status"] == "FAILED" and res["error_code"] == "FLOW_NOT_FOUND" and res["status_code"] == 404
    state["result"] = None
    assert _invoke(p)["status"] == "SUCCEEDED"  # failure was not stored in the idempotency ledger
    assert len(calls) == 2


def test_trg04_engine_error_propagates_as_failed(runner) -> None:
    _, state = runner
    state["exc"] = RuntimeError("LLM provider timeout")
    res = _invoke({"trigger_type": "STREAM_TRIGGER_RUN", "batch_id": "b1"})
    assert res["status"] == "FAILED" and res["error_code"] == "FLOW_RUN_FAILED"
    assert "LLM provider timeout" in res["error_message"]


def test_trg04_no_registered_trigger_role_fails_closed_without_running_or_writes(runner) -> None:
    calls, _ = runner
    TRIGGERS_MANAGER._triggers_by_id.clear()
    bridge = _Bridge(role="SERVICE_BASE_ROLE")  # the service/caller role must never be used silently
    res = _invoke({"trigger_type": "SCHEDULED_RUN"}, bridge)
    assert res["status"] == "FAILED" and res["status_code"] == 403 and calls == []
    assert res["active_role"] is None and bridge.telemetry == []


def test_trg04_run_is_pinned_to_persisted_trigger_role_not_service_role(runner, monkeypatch) -> None:
    import lfx_snowflake.fastapi_live_bridge as flb

    calls, state = runner
    _register(FLOW, "OLD_ROLE", task="LF_TRG_A", updated_at="2026-01-01")
    _register(FLOW, "NEWEST_ROLE", task="LF_TRG_B", updated_at="2027-01-01")
    seen = {}

    async def _probe(*, flow_id, trigger_payload, run_id):
        seen["pinned"] = flb.TRIGGER_RUN_ROLE.get()
        seen["bridge_role"] = flb.LIVE_BRIDGE.active_role  # what execute_sql would send
        return {"found": True, "run_id": run_id}

    monkeypatch.setattr(TRIGGERS_MANAGER, "native_flow_runner", _probe)
    res = _invoke({"trigger_type": "STREAM_TRIGGER_RUN", "task_name": "DB.SCH.LF_TRG_A"}, _Bridge(role="SERVICE_BASE_ROLE"))
    assert res["status"] == "SUCCEEDED" and res["active_role"] == "OLD_ROLE"
    assert seen == {"pinned": "OLD_ROLE", "bridge_role": "OLD_ROLE"}
    _invoke({"trigger_type": "STREAM_TRIGGER_RUN", "batch_id": "b-new"})
    assert seen["pinned"] == "NEWEST_ROLE"  # no task name -> newest registration
    assert flb.TRIGGER_RUN_ROLE.get() is None  # nothing leaks out of the run


def test_trg04_provisioning_without_role_fails_closed() -> None:
    from lfx_snowflake.providers.registry import SnowflakeProviderError

    spec = build_schedule_trigger_ddl(flow_id=FLOW)
    with pytest.raises(SnowflakeProviderError):
        TRIGGERS_MANAGER.provision_trigger(ddl_spec=spec, node_id="n", bridge=_Bridge(role=""))


def test_trg04_unknown_flow_writes_no_telemetry(runner) -> None:
    _, state = runner
    state["result"] = {"found": False}
    bridge = _Bridge()
    assert _invoke({"trigger_type": "SCHEDULED_RUN"}, bridge)["error_code"] == "FLOW_NOT_FOUND"
    assert bridge.telemetry == []


def test_trg04_no_hardcoded_role_or_warehouse_fallbacks() -> None:
    import inspect

    src = inspect.getsource(tm.SnowflakeTriggersManager._execute_single_trigger_invocation)
    assert "SNOWFLAKE_LANGFLOW_ADMIN_ROLE" not in src and "COMPUTE_WH" not in src
    assert "simulate_failure" not in src


def test_trg04_success_is_deduplicated_on_slot(runner) -> None:
    calls, _ = runner
    p = {"trigger_type": "SCHEDULED_RUN", "scheduled_time": "2026-11-11T10:00:00Z"}
    first, second = _invoke(p), _invoke(p)
    assert second["deduplicated"] is True and second["run_id"] == first["run_id"] and len(calls) == 1


def test_trg04_runs_on_server_loop_when_given(runner) -> None:
    import asyncio
    import threading

    calls, _ = runner
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    try:
        res = TRIGGERS_MANAGER.execute_service_trigger_batch(
            {"data": [[0, FLOW, {"trigger_type": "SCHEDULED_RUN"}]]}, bridge=_Bridge(), main_loop=loop
        )["data"][0][1]
    finally:
        loop.call_soon_threadsafe(loop.stop)
        t.join(2)
    assert res["status"] == "SUCCEEDED" and calls[0]["ctx_fid"] == FLOW


def test_trg04_native_runner_targets_langflow_run_engine() -> None:
    import inspect

    src = inspect.getsource(tm._native_langflow_run)
    for needle in ("simple_run_flow", "SimplifiedAPIRequest", "FAMILY_WEBHOOK", "session.get(Flow", '"found": False'):
        assert needle in src


# --------------------------------------------------------------------------- veto item 1: components in a trigger run


def _records(df) -> list[dict]:
    if hasattr(df, "to_dict"):  # pandas-backed lfx DataFrame
        return df.to_dict("records")
    return [dict(getattr(r, "data", r)) for r in df]  # list-backed test shim


def _components():
    from lfx_snowflake.native_category_components.triggers.snowflake_alert_trigger import SnowflakeAlertTrigger
    from lfx_snowflake.native_category_components.triggers.snowflake_schedule_trigger import SnowflakeScheduleTrigger
    from lfx_snowflake.native_category_components.triggers.snowflake_stage_file_trigger import SnowflakeStageFileTrigger
    from lfx_snowflake.native_category_components.triggers.snowflake_table_change_trigger import (
        SnowflakeTableChangeTrigger,
    )

    return SnowflakeScheduleTrigger, SnowflakeTableChangeTrigger, SnowflakeStageFileTrigger, SnowflakeAlertTrigger


def test_trg04_components_never_reprovision_and_expose_payload_during_trigger_run(monkeypatch) -> None:
    sched_cls, table_cls, stage_cls, alert_cls = _components()
    provisioned: list = []
    sql: list = []
    monkeypatch.setattr(TRIGGERS_MANAGER, "provision_trigger", lambda **kw: provisioned.append(kw) or {"ok": True})
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", lambda st, **kw: sql.append(st) or {"ok": True, "rows": []})
    rows = [{"ORDER_ID": 1}, {"ORDER_ID": 2}]
    files = [{"RELATIVE_PATH": "a.pdf"}]
    token = tm.CURRENT_TRIGGER_RUN.set({"flow_id": FLOW, "trigger_type": "X", "payload": {
        "rows": rows, "files": files, "trigger_message": "from snowflake", "row_count": 2}})
    try:
        s = sched_cls()
        s._flow_id = FLOW
        assert s.build_trigger_message().text == "from snowflake"
        t = table_cls()
        t._flow_id = FLOW
        t.object_kind = "TABLE"
        assert _records(t.build_changed_rows()) == rows
        assert "2 changed row(s)" in t.build_trigger_message().text
        st = stage_cls()
        st._flow_id = FLOW
        st.stage_name = "DOCS"
        assert _records(st.build_staged_files()) == files
        a = alert_cls()
        a._flow_id = FLOW
        a.condition_sql = "SELECT 1 FROM DB.SCH.T"
        assert _records(a.build_alert_rows()) == rows
    finally:
        tm.CURRENT_TRIGGER_RUN.reset(token)
    assert provisioned == []  # no CREATE/ALTER TASK|STREAM|ALERT during a Snowflake-fired run
    assert not [q for q in sql if re.match(r"\s*(CREATE|ALTER|DROP)\b", q, re.I)]


def test_trg04_editor_runs_still_provision(monkeypatch) -> None:
    sched_cls = _components()[0]
    provisioned: list = []
    monkeypatch.setattr(TRIGGERS_MANAGER, "provision_trigger", lambda **kw: provisioned.append(kw) or {"ok": True})
    s = sched_cls()
    s._flow_id = FLOW
    s.build_trigger_message()
    assert len(provisioned) == 1


def test_trg04_trigger_run_of_another_flow_does_not_leak() -> None:
    token = tm.CURRENT_TRIGGER_RUN.set({"flow_id": FLOW, "payload": {}})
    try:
        assert tm.active_trigger_run(FLOW.replace("-", "")) is not None
        assert tm.active_trigger_run("99999999-2222-3333-4444-555555555555") is None
    finally:
        tm.CURRENT_TRIGGER_RUN.reset(token)


def test_trg04_run_scope_publishes_trigger_payload(runner, monkeypatch) -> None:
    seen = {}

    async def _probe(*, flow_id, trigger_payload, run_id):
        seen["run"] = tm.active_trigger_run(flow_id)
        return {"found": True, "run_id": run_id}

    monkeypatch.setattr(TRIGGERS_MANAGER, "native_flow_runner", _probe)
    _invoke({"trigger_type": "STAGE_FILE_TRIGGER_RUN", "files": [{"RELATIVE_PATH": "x"}]})
    assert seen["run"]["trigger_type"] == "STAGE_FILE_TRIGGER_RUN" and seen["run"]["payload"]["files"] == [{"RELATIVE_PATH": "x"}]
    assert tm.CURRENT_TRIGGER_RUN.get() is None


# --------------------------------------------------------------------------- veto item 4


def test_trg06_component_and_sync_paths_do_not_coerce_zero_interval() -> None:
    sched_cls = _components()[0]
    s = sched_cls()
    s._flow_id = FLOW
    s.schedule_type = "Minutes"
    s.interval_value = 0
    with pytest.raises(ValueError):
        s._provision_and_get_spec()
    import inspect

    src = inspect.getsource(tm)
    assert 'vals.get("interval_value") or 15' not in src
