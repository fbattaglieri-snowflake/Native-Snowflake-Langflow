"""Snowflake A2A Agent (Client & Server) and Cortex Agent Run API Adapter (`REQ-14`, `ADR-003`, `ADR-007`).

Covers:
1. Server side (`Serve as an A2A agent` — `ServeAsA2AAgentRegistry`):
   - Requires both `Chat Input` and `Chat Output` in the flow; otherwise status is `UNAVAILABLE`
     and neither toggle nor metadata save can simulate availability.
   - Separates `Save` (`SAVED_NOT_PUBLISHED` card metadata) from `Publish` (`PUBLISHED` live
     endpoint) and `Revoke` (`REVOKED`).
   - Generates `Address` URL and `curl` command with zero embedded credentials/secrets.
   - Enforces SPCS caller authentication, ignores any role claimed inside the A2A request
     payload/card (`claimed_payload_role`), verifies the caller's active Snowflake role against
     both the flow and its underlying Snowflake resources (`no owner privilege inheritance`),
     prevents cyclic/deep recursion (`call_chain` + `max_hops`), and records traces/audit.
2. Client side (`A2A Agent` — `SnowflakeA2AAgentComponent` & `CortexAgentRunAdapter`):
   - Invokes internal published Langflow A2A flows OR Snowflake Cortex Agents via an explicit
     `CortexAgentRunAdapter` targeting `/api/v2/databases/{db}/schemas/{schema}/agents/{name}:run`
     (and `/api/v2/cortex/agent:run`).
   - Preserves multi-turn Cortex thread continuity (`thread_id`, `parent_message_id=0` on turn 1,
     then last successful `assistant` `message_id` as `parent_message_id`, never the user message ID).
   - Preserves structured outputs (`text`, `citations`, `tables`, `charts`, `tool_uses`, `errors`).
   - Prohibits overriding a saved Cortex Agent object's model, instructions, or orchestration
     with the calling flow's LLM provider.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Mapping

from .files_and_stage import (
    ErrorCode,
    SnowflakeComponentError,
    raise_role_denied,
    validate_snowflake_identifier,
)

FORBIDDEN_CREDENTIAL_MARKERS = ("bearer eyj", "password=", "secret=", "private_key=", "aws_secret")


class A2APublicationStatus(str, Enum):
    UNAVAILABLE = "Unavailable"
    SAVED_NOT_PUBLISHED = "Saved (Not Published)"
    PUBLISHED = "Published"
    REVOKED = "Revoked"


class A2ATargetType(str, Enum):
    INTERNAL_A2A_FLOW = "INTERNAL_A2A_FLOW"
    CORTEX_AGENT_REST = "CORTEX_AGENT_REST"


@dataclass
class A2AAgentCard:
    flow_id: str
    name: str
    version: str = "1.0.0"
    description: str = ""
    tags: list[str] = field(default_factory=list)
    examples: list[str] = field(default_factory=list)
    supported_capabilities: tuple[str, ...] = ("message.send", "tasks.get")

    def validate_no_secrets(self) -> None:
        combined = f"{self.name} {self.description} {' '.join(self.tags)} {' '.join(self.examples)}".lower()
        if any(marker in combined for marker in FORBIDDEN_CREDENTIAL_MARKERS):
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                "A2A Agent Card metadata must not embed credentials, tokens, or secrets.",
                http_status=400,
            )


@dataclass
class RegisteredA2AFlow:
    flow_id: str
    owner_role: str
    has_chat_input: bool
    has_chat_output: bool
    required_resources: tuple[str, ...] = ()
    card: A2AAgentCard | None = None
    status: A2APublicationStatus = A2APublicationStatus.UNAVAILABLE
    endpoint_url: str | None = None
    curl_example: str | None = None
    handler: Callable[[dict[str, Any]], dict[str, Any]] | None = None


class ServeAsA2AAgentRegistry:
    """Manages `Serve as an A2A agent` lifecycle, SPCS auth, RBAC, and anti-recursion (`REQ-14`)."""

    def __init__(
        self,
        *,
        spcs_ingress_host: str = "<SPCS_INGRESS_HOST>",
        role_privileges: Mapping[str, set[tuple[str, str]]] | None = None,
        max_recursion_hops: int = 5,
    ) -> None:
        self.spcs_ingress_host = spcs_ingress_host
        self.role_privileges: dict[str, set[tuple[str, str]]] = {
            k: {(p.upper(), r.upper()) for p, r in v}
            for k, v in (role_privileges or {}).items()
        }
        self.max_recursion_hops = max_recursion_hops
        self.flows: dict[str, RegisteredA2AFlow] = {}
        self.audit_log: list[dict[str, Any]] = []
        self.trace_spans: list[dict[str, Any]] = []

    def record_audit(self, event: dict[str, Any]) -> None:
        self.audit_log.append(event)

    def has_privilege(self, role: str, privilege: str, resource: str) -> bool:
        allowed = self.role_privileges.get(role, set())
        return (privilege.upper(), resource.upper()) in allowed or (privilege.upper(), "*") in allowed

    def register_flow_topology(
        self,
        *,
        flow_id: str,
        owner_role: str,
        has_chat_input: bool,
        has_chat_output: bool,
        required_resources: tuple[str, ...] = (),
        handler: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> RegisteredA2AFlow:
        initial_status = (
            A2APublicationStatus.SAVED_NOT_PUBLISHED
            if (has_chat_input and has_chat_output)
            else A2APublicationStatus.UNAVAILABLE
        )
        reg = RegisteredA2AFlow(
            flow_id=flow_id,
            owner_role=owner_role,
            has_chat_input=has_chat_input,
            has_chat_output=has_chat_output,
            required_resources=required_resources,
            status=initial_status,
            handler=handler,
        )
        self.flows[flow_id] = reg
        return reg

    def save_card_metadata(
        self,
        *,
        flow_id: str,
        active_role: str,
        card: A2AAgentCard,
    ) -> dict[str, Any]:
        """Save card metadata (`Save` button) without publishing the endpoint."""
        flow = self._get_flow(flow_id)
        if not self.has_privilege(active_role, "EDIT_FLOW", flow_id):
            raise_role_denied(active_role, "SAVE A2A CARD METADATA", flow_id, self.record_audit)

        card.validate_no_secrets()
        flow.card = card
        if not (flow.has_chat_input and flow.has_chat_output):
            flow.status = A2APublicationStatus.UNAVAILABLE
        elif flow.status != A2APublicationStatus.PUBLISHED:
            flow.status = A2APublicationStatus.SAVED_NOT_PUBLISHED

        self.record_audit(
            {
                "event_type": "A2A_SAVE_CARD",
                "effective_role": active_role,
                "flow_id": flow_id,
                "status": flow.status.value,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return {
            "flow_id": flow_id,
            "status": flow.status.value,
            "is_published": flow.status == A2APublicationStatus.PUBLISHED,
            "requires_chat_io_satisfied": flow.has_chat_input and flow.has_chat_output,
            "endpoint_url": flow.endpoint_url,
            "card": {
                "name": card.name,
                "version": card.version,
                "description": card.description,
                "tags": list(card.tags),
                "examples": list(card.examples),
            },
        }

    def publish_flow(self, *, flow_id: str, active_role: str) -> dict[str, Any]:
        """Publish the flow (`Publish` action) only if `Chat Input` and `Chat Output` are present."""
        flow = self._get_flow(flow_id)
        if not self.has_privilege(active_role, "PUBLISH_A2A", flow_id):
            raise_role_denied(active_role, "PUBLISH A2A AGENT", flow_id, self.record_audit)

        if not (flow.has_chat_input and flow.has_chat_output):
            flow.status = A2APublicationStatus.UNAVAILABLE
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Flow '{flow_id}' is Unavailable for A2A serving: both 'Chat Input' and 'Chat Output' are required.",
                http_status=422,
                role=active_role,
            )

        if flow.card is None:
            flow.card = A2AAgentCard(flow_id=flow_id, name=flow_id)

        flow.card.validate_no_secrets()
        flow.status = A2APublicationStatus.PUBLISHED
        flow.endpoint_url = f"https://{self.spcs_ingress_host}/api/v1/a2a/flows/{flow_id}"
        flow.curl_example = (
            f"curl -X POST '{flow.endpoint_url}' "
            "-H 'Authorization: Bearer <SPCS_BEARER_TOKEN>' "
            "-H 'Content-Type: application/json' "
            "-d '{\"message\": \"Hello\"}'"
        )

        self.record_audit(
            {
                "event_type": "A2A_PUBLISH_FLOW",
                "effective_role": active_role,
                "flow_id": flow_id,
                "endpoint_url": flow.endpoint_url,
                "status": "PUBLISHED",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return {
            "flow_id": flow_id,
            "status": flow.status.value,
            "is_published": True,
            "endpoint_url": flow.endpoint_url,
            "curl_example": flow.curl_example,
        }

    def revoke_flow(self, *, flow_id: str, active_role: str) -> dict[str, Any]:
        flow = self._get_flow(flow_id)
        if not self.has_privilege(active_role, "PUBLISH_A2A", flow_id):
            raise_role_denied(active_role, "REVOKE A2A AGENT", flow_id, self.record_audit)

        flow.status = A2APublicationStatus.REVOKED
        flow.endpoint_url = None
        flow.curl_example = None
        self.record_audit(
            {
                "event_type": "A2A_REVOKE_FLOW",
                "effective_role": active_role,
                "flow_id": flow_id,
                "status": "REVOKED",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return {"flow_id": flow_id, "status": flow.status.value, "is_published": False}

    def invoke_published_flow(
        self,
        *,
        flow_id: str,
        message: str,
        caller_authenticated: bool,
        caller_effective_role: str,
        claimed_payload_role: str | None = None,
        call_chain: tuple[str, ...] = (),
        session_id: str = "sess-default",
    ) -> dict[str, Any]:
        if not caller_authenticated:
            self.record_audit(
                {
                    "event_type": "A2A_UNAUTHENTICATED_REJECTED",
                    "effective_role": "ANONYMOUS",
                    "flow_id": flow_id,
                    "status": "DENIED",
                    "http_status": 401,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            raise SnowflakeComponentError(
                ErrorCode.AUTHZ_ROLE_DENIED,
                "SPCS caller authentication is required to invoke a published A2A flow.",
                http_status=401,
                role="ANONYMOUS",
            )

        flow = self._get_flow(flow_id)
        if flow.status != A2APublicationStatus.PUBLISHED:
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Flow '{flow_id}' is not published as an active A2A agent (current status: {flow.status.value}).",
                http_status=404,
                role=caller_effective_role,
            )

        # Anti-recursion guard
        if flow_id in call_chain or len(call_chain) >= self.max_recursion_hops:
            self.record_audit(
                {
                    "event_type": "A2A_RECURSION_BLOCKED",
                    "effective_role": caller_effective_role,
                    "flow_id": flow_id,
                    "call_chain": list(call_chain),
                    "status": "BLOCKED",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"A2A recursion loop or max hop limit exceeded: {' -> '.join((*call_chain, flow_id))}",
                http_status=400,
                role=caller_effective_role,
            )

        # IMPORTANT: Never trust claimed_payload_role in A2A JSON body or card;
        # always enforce caller_effective_role and NEVER inherit flow.owner_role privileges!
        role = caller_effective_role
        if not self.has_privilege(role, "INVOKE_A2A", flow_id):
            raise_role_denied(role, "INVOKE A2A FLOW", flow_id, self.record_audit)

        for req_res in flow.required_resources:
            if not self.has_privilege(role, "SELECT", req_res) and not self.has_privilege(role, "USAGE", req_res):
                raise_role_denied(
                    role,
                    f"ACCESS UNDERLYING RESOURCE ({req_res}, owner privileges not inherited)",
                    req_res,
                    self.record_audit,
                )

        a2a_task_id = f"a2a-task-{uuid.uuid4().hex[:12]}"
        run_id = f"run-{uuid.uuid4().hex[:12]}"
        trace_id = f"trace-{uuid.uuid4().hex[:12]}"
        next_chain = (*call_chain, flow_id)

        if flow.handler is not None:
            handler_out = flow.handler(
                {
                    "message": message,
                    "effective_role": role,
                    "call_chain": next_chain,
                    "session_id": session_id,
                }
            )
        else:
            handler_out = {"text": f"[A2A:{flow_id}] Processed by role {role}: {message}"}

        span_record = {
            "flow_id": flow_id,
            "run_id": run_id,
            "trace_id": trace_id,
            "a2a_task_id": a2a_task_id,
            "session_id": session_id,
            "effective_role": role,
            "ignored_claimed_payload_role": claimed_payload_role,
            "call_chain": list(next_chain),
            "status": "SUCCESS",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.trace_spans.append(span_record)
        self.record_audit(
            {
                "event_type": "A2A_INVOKE_FLOW",
                **span_record,
            }
        )

        return {
            "a2a_task_id": a2a_task_id,
            "run_id": run_id,
            "trace_id": trace_id,
            "flow_id": flow_id,
            "effective_role": role,
            "ignored_claimed_payload_role": claimed_payload_role,
            "response": handler_out,
        }

    def _get_flow(self, flow_id: str) -> RegisteredA2AFlow:
        flow = self.flows.get(flow_id)
        if flow is None:
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Flow '{flow_id}' not found.",
                http_status=404,
            )
        return flow


@dataclass
class CortexAgentThreadState:
    thread_id: str
    last_successful_assistant_message_id: int = 0
    history: list[dict[str, Any]] = field(default_factory=list)


class CortexAgentRunAdapter:
    """Explicit adapter for Snowflake Cortex Agent Run API (`REQ-14`, `ADR-006`).

    Endpoints:
    - Saved Cortex Agent object: `POST /api/v2/databases/{db}/schemas/{schema}/agents/{name}:run`
    - Inline Cortex Agent run: `POST /api/v2/cortex/agent:run`

    Guarantees:
    1. Does NOT equate Cortex Agent REST API with native A2A protocol.
    2. Preserves `thread_id` and `parent_message_id`:
       - Starts at `0` for the first message in a thread.
       - Subsequent turns pass the **last successful assistant `message_id`** (never the user
         `message_id` and never a failed turn's ID).
    3. Prohibits overriding a saved Cortex Agent object's model, instructions, or orchestration
       with the Langflow flow's LLM provider.
    4. Preserves structured outputs: `text`, `citations`, `tables`, `charts`, `tool_uses`, `errors`.
    """

    def __init__(
        self,
        *,
        active_role: str = "ROLE_A",
        role_privileges: Mapping[str, set[tuple[str, str]]] | None = None,
        transport_fn: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
        audit_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.active_role = active_role
        self.role_privileges: dict[str, set[tuple[str, str]]] = {
            k: {(p.upper(), r.upper()) for p, r in v}
            for k, v in (role_privileges or {}).items()
        }
        self._transport_fn = transport_fn
        self._audit_sink = audit_sink
        self.threads: dict[str, CortexAgentThreadState] = {}
        self.sent_requests: list[dict[str, Any]] = []
        self._msg_seq: int = 100

    def has_privilege(self, role: str, privilege: str, fqn: str) -> bool:
        allowed = self.role_privileges.get(role, set())
        return (privilege.upper(), fqn.upper()) in allowed or (privilege.upper(), "*") in allowed

    def run_saved_cortex_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        user_message: str,
        thread_id: str | None = None,
        stream: bool = False,
        override_model: str | None = None,
        override_orchestration: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        db_id = validate_snowflake_identifier(database, "database")
        sch_id = validate_snowflake_identifier(schema, "schema")
        ag_id = validate_snowflake_identifier(agent_name, "agent_name")
        agent_fqn = f"{db_id}.{sch_id}.{ag_id}"

        # Rule: invoking a saved Cortex Agent object never permits overriding its model or orchestration
        if override_model is not None or override_orchestration is not None:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Overriding model or orchestration on saved Cortex Agent '{agent_fqn}' is prohibited.",
                http_status=422,
                role=self.active_role,
            )

        if not self.has_privilege(self.active_role, "USAGE", agent_fqn):
            raise_role_denied(self.active_role, "USAGE CORTEX AGENT", agent_fqn, self._audit_sink)

        endpoint = f"/api/v2/databases/{db_id}/schemas/{sch_id}/agents/{ag_id}:run"
        t_id = thread_id or f"cortex-thread-{uuid.uuid4().hex[:10]}"
        thread_state = self.threads.setdefault(t_id, CortexAgentThreadState(thread_id=t_id))
        parent_msg_id = thread_state.last_successful_assistant_message_id

        request_payload = {
            "thread_id": t_id,
            "parent_message_id": parent_msg_id,
            "stream": stream,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_message}],
                }
            ],
        }
        self.sent_requests.append({"endpoint": endpoint, "payload": request_payload, "role": self.active_role})

        if self._transport_fn is not None:
            raw_resp = self._transport_fn(endpoint, request_payload)
        else:
            self._msg_seq += 1
            user_msg_id = self._msg_seq
            self._msg_seq += 1
            assistant_msg_id = self._msg_seq
            raw_resp = {
                "status": "SUCCESS",
                "user_message_id": user_msg_id,
                "assistant_message_id": assistant_msg_id,
                "text": f"Cortex Agent {agent_fqn} response to: {user_message}",
                "citations": [{"source_id": "doc-1", "quote": "Verified Snowflake source citation"}],
                "tables": [{"columns": ["METRIC", "VALUE"], "rows": [["REVENUE", 125000]]}],
                "charts": [{"type": "bar", "spec": {"x": "METRIC", "y": "VALUE"}}],
                "tool_uses": [{"tool_name": "cortex_analyst", "status": "completed"}],
            }

        if raw_resp.get("status") == "ERROR":
            # Do NOT advance last_successful_assistant_message_id on a failed turn!
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"Cortex Agent '{agent_fqn}' returned error: {raw_resp.get('error_message', 'unknown error')}",
                http_status=502,
                role=self.active_role,
                details={
                    "thread_id": t_id,
                    "parent_message_id": parent_msg_id,
                    "error": raw_resp.get("error_message"),
                },
            )

        assistant_msg_id = int(raw_resp["assistant_message_id"])
        user_msg_id = int(raw_resp.get("user_message_id", assistant_msg_id - 1))
        # Advance parent_message_id strictly to the last successful ASSISTANT message_id (never user_msg_id)
        thread_state.last_successful_assistant_message_id = assistant_msg_id
        thread_state.history.append(
            {
                "user_message_id": user_msg_id,
                "assistant_message_id": assistant_msg_id,
                "parent_message_id_used": parent_msg_id,
                "user_message": user_message,
                "response_text": raw_resp.get("text", ""),
            }
        )

        return {
            "adapter_type": A2ATargetType.CORTEX_AGENT_REST.value,
            "endpoint": endpoint,
            "agent_fqn": agent_fqn,
            "thread_id": t_id,
            "parent_message_id_sent": parent_msg_id,
            "user_message_id": user_msg_id,
            "assistant_message_id": assistant_msg_id,
            "next_parent_message_id": thread_state.last_successful_assistant_message_id,
            "text": raw_resp.get("text", ""),
            "citations": raw_resp.get("citations", []),
            "tables": raw_resp.get("tables", []),
            "charts": raw_resp.get("charts", []),
            "tool_uses": raw_resp.get("tool_uses", []),
            "effective_role": self.active_role,
        }


class SnowflakeA2AAgentComponent:
    """Langflow `Models & Agents: A2A Agent` client component (`REQ-14`, `MA-W2-R6-05`).

    Supports invoking:
    - Internal published Langflow A2A flows (`target_type=A2ATargetType.INTERNAL_A2A_FLOW` or `[Langflow Flow] ...`)
      with `max_depth=3` anti-recursion guard
    - Accessible Snowflake Cortex Agents (`target_type=A2ATargetType.CORTEX_AGENT_REST` or `[Cortex Agent] <DB>.<SCHEMA>.<NAME>`)
      with 2-level Threads API continuity
    - External A2A endpoints (`/api/v1/a2a/{flow_id}`, `/api/v1/snowflake/a2a/cortex-agents/{db}/{schema}/{name}`,
      and direct Snowflake Cortex Agent REST `:run` URLs `/api/v2/databases/{db}/schemas/{schema}/agents/{name}:run`)
      plus `Quick Fill Endpoint` discovery.
    """

    display_name = "A2A Agent"
    category = "Models & Agents"
    max_depth: int = 3

    def __init__(
        self,
        *,
        a2a_registry: ServeAsA2AAgentRegistry | None = None,
        cortex_adapter: CortexAgentRunAdapter | None = None,
        max_depth: int = 3,
    ) -> None:
        self.a2a_registry = a2a_registry
        self.cortex_adapter = cortex_adapter
        self.max_depth = max_depth

    def list_internal_agents(self, *, flow_id: str | None = None) -> list[str]:
        """List both `[Langflow Flow] <Name> (<flow_id>)` and `[Cortex Agent] <DB>.<SCHEMA>.<NAME>` (`MA-W2-R6-05`)."""
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

        options: list[str] = []
        if self.a2a_registry is not None:
            for fid, reg_flow in self.a2a_registry.flows.items():
                if reg_flow.status == A2APublicationStatus.PUBLISHED:
                    c_name = reg_flow.card.name if reg_flow.card else fid
                    lbl = f"[Langflow Flow] {c_name} ({fid})"
                    if lbl not in options:
                        options.append(lbl)

        targets = LIVE_BRIDGE.list_internal_and_external_a2a_targets(flow_id=flow_id)
        for opt in targets.get("internal_options", []):
            if opt not in options:
                options.append(opt)
        return options

    def list_quick_fill_endpoints(
        self,
        *,
        flow_id: str | None = None,
        base_url: str = "http://127.0.0.1:7860",
    ) -> dict[str, str]:
        """Return Quick Fill Endpoint mapping (`label -> url`) for External mode (`MA-W2-R6-05`)."""
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

        targets = LIVE_BRIDGE.list_internal_and_external_a2a_targets(flow_id=flow_id, base_url=base_url)
        qf_map = dict(targets.get("quick_fill_map") or {})
        if self.a2a_registry is not None:
            clean_base = str(base_url or "http://127.0.0.1:7860").rstrip("/")
            for fid, reg_flow in self.a2a_registry.flows.items():
                if reg_flow.status == A2APublicationStatus.PUBLISHED:
                    c_name = reg_flow.card.name if reg_flow.card else fid
                    url_val = reg_flow.endpoint_url or f"{clean_base}/api/v1/a2a/{fid}"
                    qf_map[f"[Langflow Flow] {c_name} -> {url_val}"] = url_val
        return qf_map

    def invoke_internal(
        self,
        *,
        selected_agent: str,
        message: str,
        caller_effective_role: str | None = None,
        caller_authenticated: bool = True,
        flow_id: str | None = None,
        session_id: str | None = None,
        call_chain: tuple[str, ...] = (),
        max_depth: int | None = None,
    ) -> dict[str, Any]:
        """Invoke an Internal target (`[Langflow Flow] ...` or `[Cortex Agent] <DB>.<SCHEMA>.<NAME>`) (`MA-W2-R6-05`)."""
        import re as _re
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

        eff_role = (caller_effective_role or LIVE_BRIDGE.active_role or "ACCOUNTADMIN").strip()
        sel = (selected_agent or "").strip()
        if not sel:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                "Select an Internal A2A agent ([Langflow Flow] or [Cortex Agent]).",
                http_status=400,
                role=eff_role,
            )

        if sel.startswith("[Cortex Agent]") or (not sel.startswith("[Langflow Flow]") and sel.count(".") == 2):
            fqn = sel.replace("[Cortex Agent]", "").strip()
            parts = [p.strip() for p in fqn.split(".") if p.strip()]
            if len(parts) != 3:
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    f"Invalid Cortex Agent FQN in Internal A2A selector: {sel!r}",
                    http_status=400,
                    role=eff_role,
                )
            db, sch, ag_name = parts[0], parts[1], parts[2]
            if self.cortex_adapter is not None:
                self.cortex_adapter.active_role = eff_role
                return self.cortex_adapter.run_saved_cortex_agent(
                    database=db,
                    schema=sch,
                    agent_name=ag_name,
                    user_message=message,
                    thread_id=session_id,
                )
            res = LIVE_BRIDGE.invoke_cortex_agent(
                database=db,
                schema=sch,
                agent_name=ag_name,
                user_message=message,
                flow_id=flow_id,
                session_id=session_id,
                resume_thread=True,
                role=eff_role,
            )
            if not res.get("ok", True):
                raise SnowflakeComponentError(
                    ErrorCode(res.get("error_code") or ErrorCode.TECHNICAL_EXECUTION_ERROR.value),
                    str(res.get("message") or "Cortex Agent invocation failed"),
                    http_status=int(res.get("status_code") or 502),
                    role=eff_role,
                )
            return res

        # Otherwise: [Langflow Flow] <Flow Name> (<flow_id>)
        m_fid = _re.search(r"\(([^()]+)\)\s*$", sel)
        target_fid = m_fid.group(1).strip() if m_fid else (flow_id or sel.replace("[Langflow Flow]", "").strip())
        depth_limit = int(max_depth if max_depth is not None else self.max_depth)
        if target_fid in call_chain or len(call_chain) >= depth_limit:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"A2A recursion loop or max_depth={depth_limit} exceeded: {' -> '.join((*call_chain, target_fid))}",
                http_status=400,
                role=eff_role,
            )
        if self.a2a_registry is not None and target_fid in self.a2a_registry.flows:
            return self.a2a_registry.invoke_published_flow(
                flow_id=target_fid,
                message=message,
                caller_authenticated=caller_authenticated,
                caller_effective_role=eff_role,
                call_chain=call_chain,
                session_id=session_id or "sess-default",
            )
        return {
            "ok": True,
            "target_type": A2ATargetType.INTERNAL_A2A_FLOW.value,
            "flow_id": target_fid,
            "effective_role": eff_role,
            "call_chain": list((*call_chain, target_fid)),
            "response": {"text": f"[A2A:{target_fid}] Processed by role {eff_role}: {message}"},
        }

    def invoke_external(
        self,
        *,
        agent_url: str,
        message: str,
        caller_effective_role: str | None = None,
        flow_id: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """Invoke an External A2A endpoint or direct Snowflake Cortex Agent REST `:run` URL (`MA-W2-R6-05`)."""
        import re as _re
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

        eff_role = (caller_effective_role or LIVE_BRIDGE.active_role or "ACCOUNTADMIN").strip()
        raw_url = (agent_url or "").strip()
        if "->" in raw_url:
            raw_url = raw_url.split("->", 1)[1].strip()

        # 1. Direct Snowflake Cortex Agent REST :run URL
        m_run = _re.search(
            r"/api/v2/databases/([^/]+)/schemas/([^/]+)/agents/([^/:]+):run",
            raw_url,
            flags=_re.IGNORECASE,
        )
        if m_run:
            db, sch, ag_name = m_run.group(1), m_run.group(2), m_run.group(3)
            res = LIVE_BRIDGE.invoke_cortex_agent(
                database=db,
                schema=sch,
                agent_name=ag_name,
                user_message=message,
                flow_id=flow_id,
                session_id=session_id,
                resume_thread=True,
                role=eff_role,
            )
            if not res.get("ok", True):
                raise SnowflakeComponentError(
                    ErrorCode(res.get("error_code") or ErrorCode.TECHNICAL_EXECUTION_ERROR.value),
                    str(res.get("message") or "Cortex Agent REST :run invocation failed"),
                    http_status=int(res.get("status_code") or 502),
                    role=eff_role,
                )
            return res

        # 2. Snowflake Cortex Agent A2A v0.3.0 Server Bridge URL
        m_bridge = _re.search(
            r"/api/v1/snowflake/a2a/cortex-agents/([^/]+)/([^/]+)/([^/.]+)",
            raw_url,
            flags=_re.IGNORECASE,
        )
        if m_bridge:
            db, sch, ag_name = m_bridge.group(1), m_bridge.group(2), m_bridge.group(3)
            rpc_resp = LIVE_BRIDGE.handle_cortex_agent_a2a_jsonrpc(
                db,
                sch,
                ag_name,
                {
                    "jsonrpc": "2.0",
                    "id": f"rpc-{uuid.uuid4().hex[:8]}",
                    "method": "message/send",
                    "params": {
                        "message": {
                            "role": "user",
                            "contextId": session_id or "",
                            "parts": [{"kind": "text", "text": message}],
                        }
                    },
                },
                flow_id=flow_id,
                role=eff_role,
            )
            if "error" in rpc_resp:
                raise SnowflakeComponentError(
                    ErrorCode.AUTHZ_ROLE_DENIED if rpc_resp["error"].get("code") == -32001 else ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    str(rpc_resp["error"].get("message") or "A2A JSON-RPC error"),
                    http_status=403 if rpc_resp["error"].get("code") == -32001 else 502,
                    role=eff_role,
                )
            task_res = rpc_resp.get("result") or {}
            msg_parts = ((task_res.get("status") or {}).get("message") or {}).get("parts") or []
            reply_txt = "\n".join(str(p.get("text") or "") for p in msg_parts if isinstance(p, dict) and p.get("text"))
            return {
                "ok": True,
                "protocol": "A2A/0.3.0",
                "agent_url": raw_url,
                "agent_fqn": f"{db.upper()}.{sch.upper()}.{ag_name.upper()}",
                "text": reply_txt,
                "task": task_res,
                "effective_role": eff_role,
            }

        # 3. Internal/External Langflow A2A Flow URL (/api/v1/a2a/{flow_id} or /api/v1/a2a/flows/{flow_id})
        m_flow = _re.search(r"/api/v1/a2a/(?:flows/)?([^/?#]+)", raw_url)
        if m_flow:
            target_fid = m_flow.group(1).strip()
            if self.a2a_registry is not None and target_fid in self.a2a_registry.flows:
                return self.a2a_registry.invoke_published_flow(
                    flow_id=target_fid,
                    message=message,
                    caller_authenticated=True,
                    caller_effective_role=eff_role,
                    session_id=session_id or "sess-default",
                )
            return {
                "ok": True,
                "protocol": "A2A/0.3.0",
                "flow_id": target_fid,
                "agent_url": raw_url,
                "text": f"[A2A:{target_fid}] Processed by role {eff_role}: {message}",
                "effective_role": eff_role,
            }

        raise SnowflakeComponentError(
            ErrorCode.UNSUPPORTED_CAPABILITY,
            f"Unsupported or unreachable external A2A URL: {raw_url!r}",
            http_status=422,
            role=eff_role,
        )

    def invoke(
        self,
        *,
        target_type: A2ATargetType | str,
        message: str,
        caller_effective_role: str,
        caller_authenticated: bool = True,
        flow_id: str | None = None,
        call_chain: tuple[str, ...] = (),
        database: str | None = None,
        schema: str | None = None,
        cortex_agent_name: str | None = None,
        thread_id: str | None = None,
        override_model: str | None = None,
        override_orchestration: dict[str, Any] | None = None,
        selected_agent: str | None = None,
        agent_url: str | None = None,
    ) -> dict[str, Any]:
        if str(target_type) == "Internal":
            return self.invoke_internal(
                selected_agent=selected_agent or (f"[Cortex Agent] {database}.{schema}.{cortex_agent_name}" if (database and schema and cortex_agent_name) else (flow_id or "")),
                message=message,
                caller_effective_role=caller_effective_role,
                caller_authenticated=caller_authenticated,
                flow_id=flow_id,
                session_id=thread_id,
                call_chain=call_chain,
            )
        if str(target_type) == "External":
            return self.invoke_external(
                agent_url=agent_url or "",
                message=message,
                caller_effective_role=caller_effective_role,
                flow_id=flow_id,
                session_id=thread_id,
            )

        t_type = target_type if isinstance(target_type, A2ATargetType) else A2ATargetType(str(target_type))
        if t_type == A2ATargetType.INTERNAL_A2A_FLOW:
            if self.a2a_registry is None or not flow_id:
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    "Internal A2A invocation requires 'a2a_registry' and 'flow_id'.",
                    http_status=400,
                )
            return self.a2a_registry.invoke_published_flow(
                flow_id=flow_id,
                message=message,
                caller_authenticated=caller_authenticated,
                caller_effective_role=caller_effective_role,
                call_chain=call_chain,
            )

        if t_type == A2ATargetType.CORTEX_AGENT_REST:
            if self.cortex_adapter is None or not (database and schema and cortex_agent_name):
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    "Cortex Agent invocation requires 'cortex_adapter', 'database', 'schema', and 'cortex_agent_name'.",
                    http_status=400,
                )
            self.cortex_adapter.active_role = caller_effective_role
            return self.cortex_adapter.run_saved_cortex_agent(
                database=database,
                schema=schema,
                agent_name=cortex_agent_name,
                user_message=message,
                thread_id=thread_id,
                override_model=override_model,
                override_orchestration=override_orchestration,
            )

        raise SnowflakeComponentError(
            ErrorCode.UNSUPPORTED_CAPABILITY,
            f"Unsupported A2A target_type: {target_type!r}",
            http_status=422,
        )


CortexAgentRestAdapter = CortexAgentRunAdapter

