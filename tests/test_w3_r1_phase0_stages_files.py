"""W3-R1 Phase 0 - stages/files truth tests (AC-02, AC-03, AC-09, A8 security, D-12 b).

The opt-in integration test runs only with SFLF_IT=1 and reads stage names from
SFLF_IT_INTERNAL_STAGE / SFLF_IT_EXTERNAL_STAGE (fully qualified, never hard-coded).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_canonical_fk import (
    _clean_stage_options,
    _list_files_on_stage,
    _stage_file_name_options,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_stage_files import (
    _stage_fqn_options,
    _stage_options,
)
from lfx_snowflake.native_category_components.triggers import (
    SnowflakeAlertTrigger,
    SnowflakeScheduleTrigger,
    SnowflakeStageFileTrigger,
    SnowflakeTableChangeTrigger,
)
from lfx_snowflake.triggers_manager import get_trigger_stage_objects

REPO_ROOT = Path(__file__).resolve().parents[1]
BRIDGE_SRC = REPO_ROOT / "src" / "lfx_snowflake" / "fastapi_live_bridge.py"
JS_PATCH_PATH = REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js"
STAGE = "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE"


def _ok(rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"ok": True, "status_code": 200, "rows": rows or [], "row_count": len(rows or [])}


@pytest.fixture
def clean_bridge(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> LiveSnowflakeBridge:
    monkeypatch.setenv("LANGFLOW_DATABASE_URL", f"sqlite:///{tmp_path / 'langflow_clean.db'}")
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    bridge.issued_sql = []  # type: ignore[attr-defined]

    def _empty_sql(statement: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        bridge.issued_sql.append(statement)  # type: ignore[attr-defined]
        stmt_u = (statement or "").strip().upper()
        if stmt_u.startswith("SHOW DATABASES"):
            return _ok([{"name": "SNOWFLAKE_LANGFLOW"}])
        if stmt_u.startswith("SHOW SCHEMAS"):
            return _ok([{"name": "CORE"}, {"name": "PUBLIC"}])
        return _ok()

    monkeypatch.setattr(bridge, "execute_sql", _empty_sql)
    monkeypatch.setattr(bridge, "check_write_privilege_or_deny", lambda *a, **k: None)
    for mod in (
        "lfx_snowflake.fastapi_live_bridge",
        "lfx_snowflake.native_category_components.files_and_knowledge.snowflake_stage_files",
        "lfx_snowflake.native_category_components.files_and_knowledge.snowflake_canonical_fk",
    ):
        monkeypatch.setattr(f"{mod}.LIVE_BRIDGE", bridge)
    return bridge


class TestAC02StageListMatchesShowStages:
    def test_fresh_bridge_has_no_seeded_stages_files_or_volumes(self, clean_bridge: LiveSnowflakeBridge) -> None:
        assert clean_bridge.stages_registry == []
        assert clean_bridge.stage_files_registry == {}
        assert clean_bridge.external_volumes_registry == []
        vis = clean_bridge.selected_context.get("visible_objects") or {}
        assert vis.get("stages") == []
        assert vis.get("external_volumes") == []
        assert "financial_operations.yaml" not in BRIDGE_SRC.read_text(encoding="utf-8")
        assert "q4_margin_policy.pdf" not in BRIDGE_SRC.read_text(encoding="utf-8")

    def test_empty_show_stages_yields_zero_stages_everywhere(self, clean_bridge: LiveSnowflakeBridge) -> None:
        disc = clean_bridge.discover_catalog(
            database="SNOWFLAKE_LANGFLOW",
            schema="CORE",
            databases=["SNOWFLAKE_LANGFLOW"],
            schemas_by_db={"SNOWFLAKE_LANGFLOW": ["CORE", "PUBLIC"]},
            force_refresh=True,
        )
        assert disc["objects_by_db_schema"]["SNOWFLAKE_LANGFLOW.CORE"]["stages"] == []
        assert disc["stage_names"] == []
        assert clean_bridge.get_context_objects("SNOWFLAKE_LANGFLOW", "CORE", "stages") == []
        assert clean_bridge.get_context_objects("SNOWFLAKE_LANGFLOW", "PUBLIC", "stages") == []
        listed = clean_bridge.list_stages_for_context()
        assert listed["stages"] == []
        assert listed["stage_names"] == []
        assert listed["storage_integrations"] == []
        comp_opts = [s for s in _stage_options("SNOWFLAKE_LANGFLOW", "CORE") if not s.startswith(("+", "["))]
        assert comp_opts == []
        assert _stage_fqn_options() == []
        assert _clean_stage_options("SNOWFLAKE_LANGFLOW", "CORE") == []

    def test_show_stages_exact_match_with_real_stage_type(
        self, clean_bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _sql(statement: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
            stmt_u = statement.strip().upper()
            if "SHOW STAGES IN SCHEMA SNOWFLAKE_LANGFLOW.CORE" in stmt_u:
                return _ok(
                    [
                        {"name": "STAGE_ALPHA", "type": "INTERNAL"},
                        {"name": "STAGE_EXT", "type": "EXTERNAL", "url": "s3://example-bucket/path/"},
                    ]
                )
            return _ok()

        monkeypatch.setattr(clean_bridge, "execute_sql", _sql)
        clean_bridge.discover_catalog(
            database="SNOWFLAKE_LANGFLOW",
            schema="CORE",
            databases=["SNOWFLAKE_LANGFLOW"],
            schemas_by_db={"SNOWFLAKE_LANGFLOW": ["CORE", "PUBLIC"]},
            force_refresh=True,
        )
        assert clean_bridge.get_context_objects("SNOWFLAKE_LANGFLOW", "CORE", "stages") == ["STAGE_ALPHA", "STAGE_EXT"]
        assert clean_bridge.get_context_objects("SNOWFLAKE_LANGFLOW", "PUBLIC", "stages") == []
        by_name = {s["name"]: s for s in clean_bridge.list_stages_for_context()["stages"]}
        # BUG-CTX-STG-01: only INTERNAL stages are listed for the context.
        assert set(by_name) == {"STAGE_ALPHA"}
        assert by_name["STAGE_ALPHA"]["stage_type"] == "INTERNAL"
        assert by_name["STAGE_ALPHA"]["files_count"] is None


class TestAC03StageFilesHonesty:
    def test_upload_is_rejected_and_never_fakes_success(self, clean_bridge: LiveSnowflakeBridge) -> None:
        res = clean_bridge.upload_stage_file(stage_fqn=STAGE, file_name="probe.txt", content_text="hello world")
        assert res["ok"] is False
        assert int(res["status_code"]) >= 400
        assert "Upload is available after the identity upgrade" in res["message"]
        assert not any(s.strip().upper().startswith("PUT") for s in clean_bridge.issued_sql)  # type: ignore[attr-defined]
        assert clean_bridge.stage_files_registry == {}
        listed = clean_bridge.list_stage_files(STAGE)
        assert listed["ok"] is True
        assert listed["files"] == []
        assert listed["total"] == 0

    def test_upload_rejected_for_external_stage(
        self, clean_bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _sql(statement: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
            if statement.upper().startswith("SHOW STAGES LIKE 'EXT_STG'"):
                return _ok([{"name": "EXT_STG", "type": "EXTERNAL"}])
            return _ok()

        monkeypatch.setattr(clean_bridge, "execute_sql", _sql)
        res = clean_bridge.upload_stage_file(
            stage_fqn="@SNOWFLAKE_LANGFLOW.CORE.EXT_STG", file_name="a.txt", content_text="x"
        )
        assert res["ok"] is False
        assert res["error_code"] == "EXTERNAL_STAGE_UPLOAD_UNSUPPORTED"
        assert "PUT only works on internal stages" in res["message"]

    def test_list_returns_only_real_list_rows_and_fails_closed(
        self, clean_bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            clean_bridge,
            "execute_sql",
            lambda s, *a, **k: _ok([{"name": "ext_stg/docs/a.pdf", "size": 5, "md5": "m", "last_modified": "t"}]),
        )
        res = clean_bridge.list_stage_files("@SNOWFLAKE_LANGFLOW.CORE.EXT_STG")
        assert [f["path"] for f in res["files"]] == ["docs/a.pdf"]
        assert _list_files_on_stage("@SNOWFLAKE_LANGFLOW.CORE.EXT_STG")[0]["name"] == "ext_stg/docs/a.pdf"

        monkeypatch.setattr(clean_bridge, "execute_sql", lambda s, *a, **k: {"ok": False, "message": "denied"})
        res = clean_bridge.list_stage_files("@SNOWFLAKE_LANGFLOW.CORE.EXT_STG")
        assert res["ok"] is False and res["files"] == []
        assert _list_files_on_stage("@SNOWFLAKE_LANGFLOW.CORE.EXT_STG") == []
        assert _stage_file_name_options("@SNOWFLAKE_LANGFLOW.CORE.EXT_STG") == []

    def test_delete_and_refresh_report_real_sql_failure(
        self, clean_bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(clean_bridge, "execute_sql", lambda s, *a, **k: {"ok": False, "status_code": 403, "message": "no"})
        d = clean_bridge.delete_stage_file(STAGE, "docs/a.pdf")
        assert d["ok"] is False and d["status_code"] == 403 and d["removed_path"] is None
        r = clean_bridge.refresh_stage_directory(STAGE)
        assert r["ok"] is False and r["error_code"] == "REFRESH_STAGE_FAILED"


class TestA8StageSecurity:
    @pytest.mark.parametrize(
        "bad_fqn",
        [
            "@DB.SCH.STG/x; DROP TABLE T",
            "@DB.SCH.\"STG\"",
            "@DB.SCH.STG' OR '1'='1",
            "@A.B.C.D",
            "",
        ],
    )
    def test_invalid_stage_fqn_rejected_without_sql(self, clean_bridge: LiveSnowflakeBridge, bad_fqn: str) -> None:
        for res in (
            clean_bridge.delete_stage_file(bad_fqn, "a.txt"),
            clean_bridge.list_stage_files(bad_fqn),
            clean_bridge.refresh_stage_directory(bad_fqn),
            clean_bridge.upload_stage_file(stage_fqn=bad_fqn, file_name="a.txt", content_text="x"),
        ):
            assert res["ok"] is False
            assert res["status_code"] == 400
        assert clean_bridge.issued_sql == []  # type: ignore[attr-defined]

    @pytest.mark.parametrize("bad_path", ["../secret", "a/../../b", "x';REMOVE @OTHER;--", "a\nb", "a;b"])
    def test_invalid_remove_path_rejected(self, clean_bridge: LiveSnowflakeBridge, bad_path: str) -> None:
        res = clean_bridge.delete_stage_file(STAGE, bad_path)
        assert res["ok"] is False and res["error_code"] == "INVALID_STAGE_INPUT"
        assert clean_bridge.issued_sql == []  # type: ignore[attr-defined]

    def test_remove_sql_is_quoted_and_validated(self, clean_bridge: LiveSnowflakeBridge) -> None:
        res = clean_bridge.delete_stage_file(STAGE, "docs/a file.pdf")
        assert res["ok"] is True
        assert res["sql"] == "REMOVE '@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE/docs/a file.pdf';"

    def test_upload_does_not_write_shared_tmp(self) -> None:
        src = BRIDGE_SRC.read_text(encoding="utf-8")
        assert "/tmp/snowflake_stage_uploads" not in src


class TestD12ExternalVolumesSeparatePanel:
    def test_external_volumes_only_from_show_external_volumes(
        self, clean_bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        assert clean_bridge.list_external_volumes_for_context()["external_volumes"] == []

        def _sql(statement: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
            if statement.upper().startswith("SHOW EXTERNAL VOLUMES"):
                return _ok([{"name": "VOL_A", "allow_writes": "true", "comment": None}])
            if statement.upper().startswith("DESCRIBE EXTERNAL VOLUME VOL_A"):
                return _ok([{"property": "STORAGE_LOCATION_1", "property_value": "{}"}])
            return _ok()

        monkeypatch.setattr(clean_bridge, "execute_sql", _sql)
        vols = clean_bridge.list_external_volumes_for_context()["external_volumes"]
        assert [v["name"] for v in vols] == ["VOL_A"]
        assert "storage_base_url" not in vols[0]
        desc = clean_bridge.describe_external_volume("VOL_A")
        assert desc["ok"] is True and desc["properties"][0]["property"] == "STORAGE_LOCATION_1"
        assert clean_bridge.describe_external_volume("VOL_A; DROP")["ok"] is False

    def test_js_has_no_invented_volume_url_and_separate_panel(self) -> None:
        js = JS_PATCH_PATH.read_text(encoding="utf-8")
        assert "snowflake-iceberg-volume" not in js
        assert "Account-level External Volume for Apache Iceberg tables" not in js
        assert "sflf-external-volumes-config-panel" in js
        assert "return { stages: outStages, external_volumes: [] };" in js
        assert "/describe" in js


class TestAC09StageFileTriggerRealStagesOnly:
    def test_stage_file_trigger_lists_only_real_stages_when_schema_empty(
        self, clean_bridge: LiveSnowflakeBridge
    ) -> None:
        assert get_trigger_stage_objects(
            database="SNOWFLAKE_LANGFLOW", schema="CORE", flow_id="clean-flow-001", bridge=clean_bridge
        ) == []
        trig = SnowflakeStageFileTrigger()
        cfg = {
            "database": {"value": "SNOWFLAKE_LANGFLOW", "options": ["SNOWFLAKE_LANGFLOW"]},
            "schema": {"value": "CORE", "options": ["CORE"]},
            "stage_name": {"value": "", "options": []},
            "compute_mode": {"value": "Serverless (Managed)"},
            "warehouse": {"value": "COMPUTE_WH", "options": ["COMPUTE_WH"], "show": False},
        }
        updated = trig.update_build_config(cfg, "CORE", "schema")
        assert updated["stage_name"]["options"] == []

    def test_five_triggers_still_instantiate(self) -> None:
        for cls in (SnowflakeScheduleTrigger, SnowflakeTableChangeTrigger, SnowflakeStageFileTrigger, SnowflakeAlertTrigger):
            inst = cls()
            assert hasattr(inst, "inputs") and hasattr(inst, "outputs")


@pytest.mark.skipif(os.getenv("SFLF_IT") != "1", reason="Set SFLF_IT=1 to run read-only stage integration checks")
class TestPhase0StagesLiveReadOnly:
    """Read-only: runs only LIST on the stages named by env vars, using the bridge's real credentials."""

    @pytest.mark.parametrize("env_name", ["SFLF_IT_INTERNAL_STAGE", "SFLF_IT_EXTERNAL_STAGE"])
    def test_list_named_stage(self, env_name: str) -> None:
        stage = os.getenv(env_name, "").strip()
        if not stage:
            pytest.skip(f"{env_name} not set")
        bridge = LiveSnowflakeBridge()
        res = bridge.list_stage_files(stage)
        assert res["ok"] is True, res.get("message")
        assert isinstance(res["files"], list)
        assert bridge.stage_files_registry == {}
