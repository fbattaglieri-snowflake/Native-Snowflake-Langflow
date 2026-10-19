from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable

from ..providers.registry import (
    ERROR_TECHNICAL_EXECUTION_ERROR,
    ERROR_UNSUPPORTED_CAPABILITY,
    SnowflakeProviderError,
)
from .data_sources import SnowflakeDataSourcesComponent


FORBIDDEN_SQL_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|COPY|PUT|GET|CALL)\b",
    re.IGNORECASE,
)


@dataclass
class BoundedSqlResultPolicy:
    max_rows_execution: int = 500
    max_rows_for_synthesis: int = 25
    max_chars_for_synthesis: int = 4000
    query_timeout_seconds: int = 60


@dataclass
class MultiProtocolNativeHistories:
    """
    Preserves distinct native histories for Chat, Responses, Messages, and Cortex Analyst
    within the same application conversation without flattening opaque/signed replay payloads.
    """

    chat: list[dict[str, Any]] = field(default_factory=list)
    responses: list[dict[str, Any]] = field(default_factory=list)
    messages: list[dict[str, Any]] = field(default_factory=list)
    analyst: list[dict[str, Any]] = field(default_factory=list)
    bounded_sql_summaries: list[dict[str, Any]] = field(default_factory=list)


class CortexAnalystService:
    """
    Dedicated Cortex Analyst service (`/api/v2/cortex/analyst/message`) — NOT a 4th proxy model.
    Enforces [G-A5]:
      - Requires explicit `Semantic View` or validated YAML model on a Snowflake Data Source
      - Forbids automatic Semantic View creation or invented relationships
      - Forbids Cortex Analyst on Snowflake Postgres or cross-engine joins
      - Separates SQL generation from governed read-only SQL execution
      - Passes bounded, authorized SQL execution results into multi-turn synthesis while keeping
        distinct native histories per protocol (`chat`, `responses`, `messages`, `analyst`)
      - Invalidates context/histories/caches on active role switch or data source change
    """

    endpoint = "/api/v2/cortex/analyst/message"

    def __init__(
        self,
        data_sources: SnowflakeDataSourcesComponent,
        policy: BoundedSqlResultPolicy | None = None,
        analyst_transport: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
        sql_executor: Callable[[str, str, dict[str, Any]], list[dict[str, Any]]] | None = None,
    ) -> None:
        self.data_sources = data_sources
        self.policy = policy or BoundedSqlResultPolicy()
        self._analyst_transport = analyst_transport
        self._sql_executor = sql_executor
        self.active_data_source_fqn: str | None = None
        self._bound_role: str = data_sources.active_role
        self._bound_epoch: int = data_sources.context_epoch
        self.histories = MultiProtocolNativeHistories()

    def bind_data_source(
        self,
        qualified_name: str,
        *,
        auto_create_semantic_view: bool = False,
        invent_relationships: bool = False,
    ) -> dict[str, Any]:
        self._sync_epoch()
        status = self.data_sources.inspect_object_readiness(
            qualified_name,
            auto_create_semantic_view=auto_create_semantic_view,
            invent_relationships=invent_relationships,
        )
        if status["engine"] != "snowflake":
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Object '{qualified_name}' runs on engine '{status['engine']}'. "
                "Cortex Analyst on Snowflake Postgres or cross-engine joins are forbidden [G-A5].",
                active_role=self.data_sources.active_role,
                operation=f"bind_analyst_data_source:{qualified_name}",
            )
        if not status["select_permitted"]:
            raise SnowflakeProviderError.role_denied(
                active_role=self.data_sources.active_role,
                operation=f"SELECT ON {qualified_name}",
                required_privilege=f"SELECT ON {qualified_name}",
            )
        if not status["analyst_semantic_ready"]:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Table/View '{qualified_name}' is visible and SELECT-able, but is NOT ready for Cortex Analyst: "
                "missing an explicit Semantic View or validated semantic YAML model. "
                "Selecting a table alone is not sufficient and automatic Semantic View creation is forbidden [G-A5].",
                active_role=self.data_sources.active_role,
                operation=f"bind_analyst_data_source:{qualified_name}",
            )

        if self.active_data_source_fqn != qualified_name:
            self.invalidate_session_context("data_source_changed")
            self.active_data_source_fqn = qualified_name
        return status

    def invalidate_session_context(self, reason: str = "role_or_source_changed") -> dict[str, Any]:
        self.histories = MultiProtocolNativeHistories()
        self._bound_role = self.data_sources.active_role
        self._bound_epoch = self.data_sources.context_epoch
        return {
            "invalidated": True,
            "reason": reason,
            "active_role": self._bound_role,
            "context_epoch": self._bound_epoch,
        }

    def _sync_epoch(self) -> None:
        if (
            self.data_sources.active_role != self._bound_role
            or self.data_sources.context_epoch != self._bound_epoch
        ):
            self.invalidate_session_context("active_role_changed")

    def generate_sql(
        self,
        user_question: str,
        *,
        qualified_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Step 1 of Analyst flow: invokes `/api/v2/cortex/analyst/message` to generate SQL and text interpretation
        without automatically executing the SQL.
        """
        self._sync_epoch()
        target_fqn = qualified_name or self.active_data_source_fqn
        if not target_fqn:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "No semantic-ready Snowflake Data Source bound to Cortex Analyst session.",
                active_role=self.data_sources.active_role,
                operation="analyst_generate_sql",
            )
        status = self.bind_data_source(target_fqn)

        user_turn = {
            "role": "user",
            "content": [{"type": "text", "text": user_question}],
        }
        request_messages = copy.deepcopy(self.histories.analyst) + [user_turn]

        request_payload: dict[str, Any] = {"messages": request_messages}
        if status.get("semantic_view_fqn"):
            request_payload["semantic_view"] = status["semantic_view_fqn"]
        elif status.get("semantic_model_yaml_stage_path"):
            request_payload["semantic_model_file"] = status["semantic_model_yaml_stage_path"]

        headers = {
            "X-Snowflake-Role": self.data_sources.active_role,
            "X-Snowflake-Context-Epoch": str(self.data_sources.context_epoch),
        }

        if self._analyst_transport is not None:
            response = self._analyst_transport(self.endpoint, request_payload, headers)
        else:
            generated_sql = f"SELECT * FROM {target_fqn} LIMIT 10"
            response = {
                "request_id": f"analyst-req-{len(self.histories.analyst) + 1}",
                "message": {
                    "role": "analyst",
                    "content": [
                        {"type": "text", "text": f"Interpreted question: {user_question}"},
                        {"type": "sql", "statement": generated_sql},
                    ],
                },
            }

        analyst_msg = response.get("message", {})
        self.histories.analyst.append(user_turn)
        self.histories.analyst.append(copy.deepcopy(analyst_msg))

        sql_statement = None
        for block in analyst_msg.get("content", []):
            if isinstance(block, dict) and block.get("type") == "sql":
                sql_statement = block.get("statement")
                break

        return {
            "request_id": response.get("request_id"),
            "request_payload": request_payload,
            "analyst_message": analyst_msg,
            "generated_sql": sql_statement,
            "executed": False,
            "active_role": self.data_sources.active_role,
            "context_epoch": self.data_sources.context_epoch,
        }

    def execute_generated_sql(
        self,
        sql_statement: str,
        *,
        run_id: str = "run-01",
        cancelled: bool = False,
    ) -> dict[str, Any]:
        """
        Step 2 of Analyst flow: governed read-only execution of the generated SQL under the active role,
        with row limit, timeout, cancellation check, and bounded summary for multi-turn synthesis.
        """
        self._sync_epoch()
        if cancelled:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                "SQL execution cancelled by caller.",
                active_role=self.data_sources.active_role,
                operation="analyst_execute_sql",
            )

        cleaned = (sql_statement or "").strip()
        if not cleaned or not (cleaned.upper().startswith("SELECT") or cleaned.upper().startswith("WITH")):
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Cortex Analyst SQL execution is strictly read-only (only SELECT / WITH queries are permitted).",
                active_role=self.data_sources.active_role,
                operation="analyst_execute_sql",
            )
        if FORBIDDEN_SQL_PATTERN.search(cleaned):
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Forbidden DML/DDL keyword detected in Cortex Analyst SQL statement.",
                active_role=self.data_sources.active_role,
                operation="analyst_execute_sql",
            )

        if not self.active_data_source_fqn:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Cannot execute Analyst SQL without an authorized bound Data Source.",
                active_role=self.data_sources.active_role,
                operation="analyst_execute_sql",
            )
        # Re-verify active role SELECT permission on bound data source
        self.bind_data_source(self.active_data_source_fqn)

        exec_meta = {
            "timeout_seconds": self.policy.max_rows_execution and self.policy.query_timeout_seconds,
            "max_rows": self.policy.max_rows_execution,
            "query_tag": json.dumps(
                {
                    "app": "snowflake-langflow",
                    "component": "cortex_analyst",
                    "run_id": run_id,
                    "role": self.data_sources.active_role,
                }
            ),
        }

        if self._sql_executor is not None:
            raw_rows = self._sql_executor(cleaned, self.data_sources.active_role, exec_meta)
        else:
            raw_rows = [{"sample_col": 1, "source": self.active_data_source_fqn}]

        exec_rows = raw_rows[: self.policy.max_rows_execution]
        synthesis_rows = exec_rows[: self.policy.max_rows_for_synthesis]
        serialized = json.dumps(synthesis_rows, default=str)
        truncated_for_synthesis = len(exec_rows) > len(synthesis_rows)
        if len(serialized) > self.policy.max_chars_for_synthesis:
            serialized = serialized[: self.policy.max_chars_for_synthesis]
            truncated_for_synthesis = True

        bounded_summary = {
            "sql": cleaned,
            "active_role": self.data_sources.active_role,
            "context_epoch": self.data_sources.context_epoch,
            "total_rows_returned": len(exec_rows),
            "synthesis_rows_included": len(synthesis_rows),
            "truncated_for_synthesis": truncated_for_synthesis,
            "bounded_payload": serialized,
            "query_tag": exec_meta["query_tag"],
        }
        self.histories.bounded_sql_summaries.append(bounded_summary)
        return {
            "rows": exec_rows,
            "bounded_summary": bounded_summary,
        }

    def build_synthesis_context_for_protocol(self, protocol: str) -> dict[str, Any]:
        """
        Provides the bounded, authorized SQL results from previous turns to the LLM synthesis step
        while keeping each protocol's native history (`chat`, `responses`, `messages`, `analyst`) distinct.
        """
        self._sync_epoch()
        if protocol not in ("chat", "responses", "messages"):
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Unsupported synthesis protocol '{protocol}'.",
                active_role=self.data_sources.active_role,
                operation="build_synthesis_context_for_protocol",
            )
        native_history = getattr(self.histories, protocol)
        return {
            "protocol": protocol,
            "active_role": self.data_sources.active_role,
            "context_epoch": self.data_sources.context_epoch,
            "native_history": copy.deepcopy(native_history),
            "analyst_history_turns": len(self.histories.analyst),
            "bounded_sql_results": copy.deepcopy(self.histories.bounded_sql_summaries),
        }
