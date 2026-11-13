from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import (
    BoolInput,
    DropdownInput,
    FloatInput,
    HandleInput,
    IntInput,
    MessageTextInput,
    MultilineInput,
    Output,
    StrInput,
)
from lfx.schema import Data, DataFrame
from lfx.schema.message import Message

from lfx_snowflake.components.llm_operations import (
    DECISION_EVALUATION_MODES,
    SUPPORTED_DECISION_MODELS,
    SUPPORTED_DECISION_QUESTION_TYPES,
    SnowflakeDecisionEngine,
)
from lfx_snowflake.context_helpers import _resolve_flow_id
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.flow_context_guard import flow_component_sql as _flow_component_sql
from lfx_snowflake.flow_context_guard import raise_if_flow_context_denied as _raise_if_flow_context_denied


def _flow_component_execute_sql(*args, **kwargs):
    """BUG-CTX-03: this component's SQL is checked against the flow's `[ ❄️ Context ]`."""
    with _flow_component_sql():
        # E2E T1: a guard refusal surfaces as a component error carrying AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT.
        return _raise_if_flow_context_denied(LIVE_BRIDGE.execute_sql(*args, **kwargs))


DEFAULT_SUPPORT_TICKET_QUESTIONS = json.dumps(
    {
        "department": {
            "type": "choice",
            "instructions": "Which team should handle this?",
            "criteria": {
                "billing": "Payment, invoice, or subscription issues",
                "technical_support": "Product bugs, outages, or configuration issues",
                "account_management": "Contract upgrades, renewals, or seat changes",
                "none_of_these": "None of the listed options apply or not stated in the input.",
            },
        },
        "frustration": {
            "type": "score",
            "instructions": "How frustrated the customer appears",
            "criteria": [
                "Calm",
                "Frustrated but civil",
                "Very angry",
            ],
        },
        "is_urgent": {
            "type": "noul",
            "instructions": "The message conveys urgency",
            "criteria": {
                "true": "Customer reports a blocker, production outage, or immediate deadline",
                "false": "Routine inquiry without time-critical impact",
            },
        },
    },
    indent=2,
)


def _decision_databases(flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id())


def _decision_schemas(database: str | None = None, flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id())


def _decision_tables(
    database: str | None = None,
    schema: str | None = None,
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    items: list[str] = []
    for cat in ("tables", "views"):
        for t in LIVE_BRIDGE.get_context_objects(database, schema, cat, flow_id=eff_fid):
            if t not in items:
                items.append(t)
    if "SUPPORT_TICKETS" not in items:
        items.append("SUPPORT_TICKETS")
    return items


class SnowflakeDecisionComponent(Component):
    display_name = "Decision"
    description = (
        "Official Snowflake Decision component powered by AI_COMPLETE with 'snowflake-decision-fast' "
        "(low latency, 2..16 choice options, 32k token limit) and 'snowflake-decision' "
        "(high accuracy, 2..32 choice options, 256 KiB limit), supporting choice, score (0..n-1), "
        "and noul questions (1..32 questions, max 256 total options), Single State & Table-Level "
        "Processing, and confidence-based routing."
    )
    icon = "Snowflake"
    name = "SnowflakeDecision"
    legacy_name = "snowflake-decision"

    inputs = [
        HandleInput(
            name="snowflake_context",
            display_name="Snowflake Context (Optional Override)",
            input_types=["Data"],
            required=False,
            advanced=True,
        ),
        DropdownInput(
            name="model",
            display_name="Decision Model",
            options=list(SUPPORTED_DECISION_MODELS),
            value="snowflake-decision-fast",
            real_time_refresh=True,
            info=(
                "Select 'snowflake-decision-fast' (default, 2..16 choice options, 32k token limit) "
                "or 'snowflake-decision' (high accuracy, 2..32 choice options, 256 KiB JSON limit)."
            ),
        ),
        DropdownInput(
            name="evaluation_mode",
            display_name="Evaluation Mode",
            options=list(DECISION_EVALUATION_MODES),
            value="Single State (Text / JSON Input)",
            real_time_refresh=True,
        ),
        MessageTextInput(
            name="state_input",
            display_name="State Input (Text or JSON Object)",
            value="Our production billing pipeline failed after invoice #42 and we need urgent technical_support.",
            show=True,
        ),
        DropdownInput(
            name="database",
            display_name="Database (Table Mode)",
            options=_decision_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
            show=False,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema (Table Mode)",
            options=_decision_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
            show=False,
        ),
        DropdownInput(
            name="table_name",
            display_name="Table / View (Table Mode)",
            options=_decision_tables("SNOWFLAKE_LANGFLOW", "CORE"),
            value="SUPPORT_TICKETS",
            real_time_refresh=True,
            combobox=True,
            show=False,
        ),
        StrInput(
            name="state_column",
            display_name="State Column Name (Table Mode)",
            value="ticket_text",
            show=False,
        ),
        IntInput(
            name="max_rows",
            display_name="Max Rows (Table Mode)",
            value=100,
            show=False,
        ),
        DropdownInput(
            name="question_type",
            display_name="Primary Question Type",
            options=list(SUPPORTED_DECISION_QUESTION_TYPES),
            value="choice",
            real_time_refresh=True,
            info="Question type: 'choice' (finite options), 'score' (2..10 ordered rubric levels 0..n-1), or 'noul' (probability ~0.08–0.92).",
        ),
        StrInput(
            name="question_id",
            display_name="Primary Question ID (1..64 chars)",
            value="department",
        ),
        MultilineInput(
            name="question_instructions",
            display_name="Primary Question Instructions",
            value="Which team should handle this?",
        ),
        MultilineInput(
            name="question_criteria_json",
            display_name="Primary Question Criteria JSON",
            value=json.dumps(
                {
                    "billing": "Payment, invoice, or subscription issues",
                    "technical_support": "Product bugs, outages, or configuration issues",
                    "account_management": "Contract upgrades, renewals, or seat changes",
                },
                indent=2,
            ),
        ),
        BoolInput(
            name="include_none_of_these",
            display_name="Include 'none_of_these' / 'not_stated' option",
            value=True,
            info="Recommended by Snowflake Decision documentation for 'choice' questions.",
        ),
        MultilineInput(
            name="questions_json",
            display_name="Multi-Question Payload JSON (1..32 Questions, max 256 Total Options)",
            value=DEFAULT_SUPPORT_TICKET_QUESTIONS,
            info=(
                "Define 1..32 independent questions ('choice', 'score', 'noul') evaluated in a single AI_COMPLETE call. "
                "When non-empty, this multi-question object is used."
            ),
        ),
        FloatInput(
            name="confidence_threshold",
            display_name="Confidence Threshold (choice / score)",
            value=0.70,
            info="Normalized confidence threshold using (n * p_max - 1) / (n - 1).",
        ),
        FloatInput(
            name="noul_threshold",
            display_name="Noul Probability Threshold (<= 0.92)",
            value=0.70,
            info="Noul probabilities stay within ~0.08–0.92; thresholds above 0.92 are rejected.",
        ),
        BoolInput(
            name="return_error_details",
            display_name="Return Error Details (return_error_details => TRUE)",
            value=True,
        ),
    ]

    outputs = [
        Output(
            display_name="Decision Object (answers)",
            name="decision_object",
            method="run_decision",
        ),
        Output(
            display_name="Automated Action Route (High Confidence)",
            name="high_confidence_route",
            method="route_high_confidence",
            # BUG-LLMOP-002: one port per route (Langflow If-Else pattern) so each branch is wired separately
            # and self.stop() deactivates only the inactive one.
            group_outputs=True,
        ),
        Output(
            display_name="Human Review / Fallback Route (Low Confidence)",
            name="low_confidence_route",
            method="route_low_confidence",
            # BUG-LLMOP-002: one port per route (Langflow If-Else pattern) so each branch is wired separately
            # and self.stop() deactivates only the inactive one.
            group_outputs=True,
        ),
        Output(
            display_name="Summary (text)",
            name="summary",
            method="run_summary",
        ),
        Output(
            display_name="Table Decisions (DataFrame)",
            name="table_decisions",
            method="run_table_decisions",
        ),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        if "model" in build_config:
            build_config["model"]["options"] = list(SUPPORTED_DECISION_MODELS)
            cur_m = build_config["model"].get("value")
            if field_name == "model" and field_value in SUPPORTED_DECISION_MODELS:
                build_config["model"]["value"] = field_value
            elif cur_m not in SUPPORTED_DECISION_MODELS:
                build_config["model"]["value"] = "snowflake-decision-fast"

        eval_mode = (
            field_value
            if field_name == "evaluation_mode" and isinstance(field_value, str)
            else build_config.get("evaluation_mode", {}).get("value")
            or getattr(self, "evaluation_mode", "Single State (Text / JSON Input)")
        )
        is_table_mode = eval_mode == "Table-Level Processing (Database.Schema.Table)"

        if "state_input" in build_config:
            build_config["state_input"]["show"] = not is_table_mode
        for tbl_field in ("database", "schema_name", "table_name", "state_column", "max_rows"):
            if tbl_field in build_config:
                build_config[tbl_field]["show"] = is_table_mode

        if "database" in build_config:
            build_config["database"]["options"] = _decision_databases(flow_id=flow_id)
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or getattr(self, "database", "SNOWFLAKE_LANGFLOW")
        )
        schemas = _decision_schemas(db, flow_id=flow_id)
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = schemas
        sch = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else "CORE")
        )
        if "table_name" in build_config:
            build_config["table_name"]["options"] = _decision_tables(db, sch, flow_id=flow_id)

        qtype = (
            field_value
            if field_name == "question_type" and isinstance(field_value, str)
            else build_config.get("question_type", {}).get("value") or getattr(self, "question_type", "choice")
        )
        if "include_none_of_these" in build_config:
            build_config["include_none_of_these"]["show"] = qtype == "choice"

        if field_name == "noul_threshold" and "noul_threshold" in build_config:
            raw_nt = float(field_value) if field_value is not None else 0.70
            if raw_nt > SnowflakeDecisionEngine.MAX_NOUL_THRESHOLD:
                build_config["noul_threshold"]["value"] = SnowflakeDecisionEngine.MAX_NOUL_THRESHOLD

        return build_config

    def _resolve_questions(self) -> dict[str, Any]:
        raw_multi = getattr(self, "questions_json", None)
        if isinstance(raw_multi, dict) and raw_multi:
            return raw_multi
        if isinstance(raw_multi, str) and raw_multi.strip():
            parsed = json.loads(raw_multi)
            if isinstance(parsed, dict) and parsed:
                return parsed

        qid = (getattr(self, "question_id", "department") or "department").strip()
        qtype = (getattr(self, "question_type", "choice") or "choice").strip().lower()
        instr = (getattr(self, "question_instructions", "Which team should handle this?") or "").strip()
        raw_crit = getattr(self, "question_criteria_json", None)
        crit = json.loads(raw_crit) if isinstance(raw_crit, str) and raw_crit.strip() else raw_crit
        entry: dict[str, Any] = {
            "type": qtype,
            "instructions": instr,
        }
        if crit is not None:
            entry["criteria"] = crit
        return {qid: entry}

    def _decision_inputs(self) -> dict[str, Any]:
        raw_state = getattr(self, "state_input", None)
        if raw_state is None:
            raw_state = getattr(self, "input_context", "")
        if isinstance(raw_state, str) and raw_state.strip().startswith("{") and raw_state.strip().endswith("}"):
            state_val: Any = json.loads(raw_state)
        else:
            state_val = raw_state or ""

        questions = self._resolve_questions()
        return dict(
            model=getattr(self, "model", "snowflake-decision-fast") or "snowflake-decision-fast",
            state=state_val,
            questions=questions,
            evaluation_mode=getattr(self, "evaluation_mode", "Single State (Text / JSON Input)")
            or "Single State (Text / JSON Input)",
            include_none_of_these=bool(getattr(self, "include_none_of_these", True)),
            confidence_threshold=float(getattr(self, "confidence_threshold", 0.70) or 0.70),
            noul_threshold=float(getattr(self, "noul_threshold", 0.70) or 0.70),
            return_error_details=bool(getattr(self, "return_error_details", True)),
            database=getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW",
            schema=getattr(self, "schema_name", "CORE") or "CORE",
            table_name=getattr(self, "table_name", "SUPPORT_TICKETS") or "SUPPORT_TICKETS",
            state_column=getattr(self, "state_column", "ticket_text") or "ticket_text",
            max_rows=int(getattr(self, "max_rows", 100) or 100),
            active_role=LIVE_BRIDGE.active_role,
        )

    def _reset_decision_cache(self) -> None:
        """Drop the cached execution (call at the start of a new build; PASSO 6 routing hooks here)."""
        self.__dict__.pop("_decision_cache", None)

    def _compute(self) -> dict[str, Any]:
        """Run AI_COMPLETE once per build and share the result with every output port (BUG-LLMOP-001).

        The cache key is the full set of decision inputs, so all ports of one build read the same
        execution and a changed input forces a fresh call. Answers and routing come only from the
        real Snowflake result; any failure raises (no heuristic fallback).
        """
        inputs = self._decision_inputs()
        key = json.dumps(inputs, sort_keys=True, default=str)
        cached = self.__dict__.get("_decision_cache")
        if cached and cached[0] == key:
            return cached[1]
        res = SnowflakeDecisionEngine.evaluate_decision(**inputs, executor=_flow_component_execute_sql)
        self.__dict__["_decision_cache"] = (key, res)
        return res

    def run_decision(self) -> Data:
        return Data(data=dict(self._compute()))

    @staticmethod
    def summarize_answers(answers: dict[str, Any]) -> str:
        """One readable line per question, for a prompt or a chat message."""
        lines = []
        for qid, ans in (answers or {}).items():
            if not isinstance(ans, dict):
                lines.append(f"{qid}: {ans}")
                continue
            conf = ans.get("confidence")
            tail = f" (confidence {float(conf):.2f})" if isinstance(conf, (int, float)) else ""
            if isinstance(ans.get("score"), (int, float)):
                legend = ans.get("legend") if isinstance(ans.get("legend"), dict) else {}
                top = max(len(legend) - 1, 0)
                level = legend.get(str(int(round(float(ans["score"])))))
                scale = f" out of {top}" if top else ""
                closest = f' - closest level: "{level}"' if level else ""
                lines.append(f"{qid}: {float(ans['score']):.2f}{scale}{closest}{tail}")
            elif "choice" in ans:
                lines.append(f"{qid}: {ans.get('choice')}{tail}")
            elif isinstance(ans.get("noul"), (int, float)):
                lines.append(f"{qid}: probability {float(ans['noul']):.2f}")
            else:
                lines.append(f"{qid}: {json.dumps(ans, default=str)}")
        return "\n".join(lines)

    def run_summary(self) -> Message:
        return Message(text=self.summarize_answers(dict(self._compute().get("answers") or {})))

    def _stop_route(self, output_name: str) -> None:
        """BUG-LLMOP-002/003: Langflow If-Else pattern; only the active branch's downstream runs.

        The route comes from the cached ``_compute()`` result (one AI_COMPLETE call per build) and the
        confidence Snowflake returns."""
        stop = getattr(self, "stop", None)
        if callable(stop) and getattr(self, "_vertex", None) is not None:  # standalone builds have no graph
            stop(output_name)

    def route_high_confidence(self) -> Data:
        res = self._compute()
        if not res["high_confidence"]:
            self._stop_route("high_confidence_route")
            return Data(data={"active": False, "route": "low_confidence", "skipped": True, "decision": res})
        self._stop_route("low_confidence_route")
        return Data(data={"active": True, "route": "high_confidence", **res})

    def route_low_confidence(self) -> Data:
        res = self._compute()
        if res["high_confidence"]:
            self._stop_route("low_confidence_route")
            return Data(data={"active": False, "route": "high_confidence", "skipped": True, "decision": res})
        self._stop_route("high_confidence_route")
        return Data(data={"active": True, "route": "low_confidence", **res})

    def run_table_decisions(self) -> DataFrame:
        res = self._compute()
        rows = res.get("rows")
        if not isinstance(rows, list):
            raise ValueError("Table-level decisions require evaluation_mode 'Table-Level Processing (Database.Schema.Table)'.")
        return DataFrame(rows)

    def get_decision_json(self) -> Message:
        return Message(text=json.dumps(self._compute(), indent=2, default=str))

    def get_selected_branch(self) -> Message:
        res = self._compute()
        return Message(text=str(res["selected_branch"]))

    def get_audit_record(self) -> Data:
        res = self._compute()
        return Data(
            data={
                "component": "snowflake-decision",
                "active_role": LIVE_BRIDGE.active_role,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "model": res["model"],
                "decision": res["decision"],
                "confidence": res["confidence"],
                "selected_branch": res["selected_branch"],
                "policy_action": res["policy_action"],
                "answers": res["answers"],
                "sql_preview": res["sql_preview"],
            }
        )
