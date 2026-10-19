"""Snowflake Message History Component (`REQ-08`, `ADR-002`, `ADR-003`).

Supports `SNOWFLAKE_POSTGRES` and `SNOWFLAKE_STANDARD_TABLE` storage backends while
preserving Langflow `Store` / `Retrieve` modes and `Messages` / `Table` output ports.

Key architectural guarantees:
1. Separates three distinct state layers per turn:
   - `visible_message`: user-facing chat text and metadata shown in `Messages` / `Table`
   - `signed_replay_envelope`: opaque/signed protocol replay blocks (`thinking.signature`,
     `redacted_thinking`, `reasoning.encrypted_content`, `ordered_items`) for the 3
     Snowflake Proxy protocols
   - `cortex_thread_ref`: Cortex Agent thread continuity (`cortex_thread_id`,
     `cortex_parent_message_id`, `last_assistant_message_id`) kept distinct from proxy state
2. Branch-on-edit / branch-on-delete for signed blocks: editing or deleting a message
   in a chain containing signed replay blocks never mutates a signed block in place;
   instead it forks a new `branch_id` (or invalidates downstream replay state) so
   cryptographic signatures are never corrupted.
3. Idempotent storage by `message_id`, deterministic ordering by `sequence_no`, and
   strict `(user_id, project_id, session_id, effective_role)` isolation with `AUTHZ_ROLE_DENIED`.
"""

from __future__ import annotations

import copy
import hashlib
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


class MessageHistoryBackendType(str, Enum):
    SNOWFLAKE_POSTGRES = "SNOWFLAKE_POSTGRES"
    SNOWFLAKE_STANDARD_TABLE = "SNOWFLAKE_STANDARD_TABLE"


class MessageHistoryMode(str, Enum):
    STORE = "Store"
    RETRIEVE = "Retrieve"


@dataclass(frozen=True)
class SignedReplayEnvelope:
    """Opaque/signed protocol replay state (`ADR-003`) kept separate from UI text."""

    protocol: str  # "Snowflake Proxy - Responses" | "Snowflake Proxy - Messages" | "Snowflake Proxy - Chat"
    thinking_signature: str | None = None
    redacted_thinking: str | None = None
    encrypted_reasoning_content: str | None = None
    ordered_items: tuple[dict[str, Any], ...] = ()
    tool_call_ids: tuple[str, ...] = ()

    @property
    def has_cryptographic_signature(self) -> bool:
        return bool(
            self.thinking_signature
            or self.redacted_thinking
            or self.encrypted_reasoning_content
        )

    @property
    def integrity_digest(self) -> str:
        payload = (
            f"{self.protocol}|{self.thinking_signature or ''}|"
            f"{self.redacted_thinking or ''}|{self.encrypted_reasoning_content or ''}|"
            f"{len(self.ordered_items)}|{','.join(self.tool_call_ids)}"
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CortexThreadReference:
    """Cortex Agent thread reference kept distinct from proxy protocol replay history."""

    cortex_thread_id: str
    cortex_parent_message_id: int = 0
    last_assistant_message_id: int | None = None
    agent_fqn: str | None = None


@dataclass
class StoredHistoryRecord:
    message_id: str
    user_id: str
    project_id: str
    session_id: str
    branch_id: str
    parent_branch_id: str | None
    sequence_no: int
    sender: str  # "User" | "AI"
    sender_name: str
    visible_text: str
    effective_role: str
    created_at: str
    signed_replay: SignedReplayEnvelope | None = None
    cortex_thread_ref: CortexThreadReference | None = None
    replay_state_status: str = "VALID"  # "VALID" | "BRANCHED_IMMUTABLE" | "INVALIDATED_BY_EDIT"
    superseded_by_branch: str | None = None


@dataclass(frozen=True)
class MessageHistoryStorageConfig:
    backend_type: MessageHistoryBackendType
    database: str
    schema: str
    table_name: str = "LANGFLOW_MESSAGE_HISTORY"
    postgres_instance: str | None = None
    postgres_role: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "database", validate_snowflake_identifier(self.database, "database"))
        object.__setattr__(self, "schema", validate_snowflake_identifier(self.schema, "schema"))
        object.__setattr__(self, "table_name", validate_snowflake_identifier(self.table_name, "table_name"))
        if self.backend_type == MessageHistoryBackendType.SNOWFLAKE_POSTGRES:
            if not self.postgres_instance or not self.postgres_role:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    "SNOWFLAKE_POSTGRES Message History requires both 'postgres_instance' and 'postgres_role'.",
                    http_status=422,
                )

    @property
    def table_fqn(self) -> str:
        return f"{self.database}.{self.schema}.{self.table_name}"


class InMemoryMessageHistoryBackend:
    """In-memory storage engine modelling both Snowflake Postgres and Snowflake Standard Table."""

    def __init__(
        self,
        *,
        active_role: str = "ROLE_A",
        snowflake_privileges: Mapping[str, set[tuple[str, str]]] | None = None,
        postgres_privileges: Mapping[str, set[tuple[str, str]]] | None = None,
    ) -> None:
        self.active_role = active_role
        self.snowflake_privileges: dict[str, set[tuple[str, str]]] = {
            k: set(v) for k, v in (snowflake_privileges or {}).items()
        }
        self.postgres_privileges: dict[str, set[tuple[str, str]]] = {
            k: set(v) for k, v in (postgres_privileges or {}).items()
        }
        # Keyed by (backend_type, table_fqn)
        self.tables: dict[tuple[str, str], list[StoredHistoryRecord]] = {}
        self.audit_log: list[dict[str, Any]] = []

    def record_audit(self, event: dict[str, Any]) -> None:
        self.audit_log.append(event)

    def check_access(self, config: MessageHistoryStorageConfig, operation: str) -> str:
        role = self.active_role
        if config.backend_type == MessageHistoryBackendType.SNOWFLAKE_STANDARD_TABLE:
            allowed = self.snowflake_privileges.get(role, set())
            if (operation.upper(), config.table_fqn.upper()) not in allowed and (operation.upper(), "*") not in allowed:
                raise_role_denied(role, f"{operation} MESSAGE HISTORY TABLE", config.table_fqn, self.record_audit)
        elif config.backend_type == MessageHistoryBackendType.SNOWFLAKE_POSTGRES:
            sf_allowed = self.snowflake_privileges.get(role, set())
            pg_inst = (config.postgres_instance or "").upper()
            if ("USAGE", pg_inst) not in sf_allowed and ("USAGE", "*") not in sf_allowed:
                raise_role_denied(role, "USAGE POSTGRES INSTANCE", pg_inst, self.record_audit)
            pg_role = config.postgres_role or ""
            pg_allowed = self.postgres_privileges.get(pg_role, set())
            if (operation.upper(), config.table_fqn.upper()) not in pg_allowed and (operation.upper(), "*") not in pg_allowed:
                raise_role_denied(
                    f"{role}/pg:{pg_role}",
                    f"POSTGRES {operation} MESSAGE HISTORY",
                    config.table_fqn,
                    self.record_audit,
                )
        return role


class SnowflakeMessageHistoryComponent:
    """Langflow `Message History` component supporting Snowflake Postgres & Standard Tables (`REQ-08`)."""

    display_name = "Message History"
    category = "Models & Agents"
    ports = ("messages", "table", "replay_state", "cortex_thread")

    def __init__(
        self,
        backend: InMemoryMessageHistoryBackend,
        config: MessageHistoryStorageConfig,
    ) -> None:
        if not isinstance(config.backend_type, MessageHistoryBackendType):
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Unsupported Message History backend: {config.backend_type!r}. "
                "Only SNOWFLAKE_POSTGRES and SNOWFLAKE_STANDARD_TABLE are supported.",
                http_status=422,
            )
        self.backend = backend
        self.config = config

    def _storage_bucket(self) -> list[StoredHistoryRecord]:
        key = (self.config.backend_type.value, self.config.table_fqn)
        return self.backend.tables.setdefault(key, [])

    def store_message(
        self,
        *,
        message_id: str,
        user_id: str,
        project_id: str,
        session_id: str,
        sender: str,
        sender_name: str,
        visible_text: str,
        branch_id: str = "main",
        signed_replay: SignedReplayEnvelope | None = None,
        cortex_thread_ref: CortexThreadReference | None = None,
    ) -> dict[str, Any]:
        role = self.backend.check_access(self.config, "INSERT")
        bucket = self._storage_bucket()

        # Idempotency check by (message_id, branch_id, user_id, project_id, session_id)
        for existing in bucket:
            if (
                existing.message_id == message_id
                and existing.branch_id == branch_id
                and existing.user_id == user_id
                and existing.project_id == project_id
                and existing.session_id == session_id
            ):
                if existing.visible_text != visible_text:
                    raise SnowflakeComponentError(
                        ErrorCode.TECHNICAL_EXECUTION_ERROR,
                        f"Idempotency conflict: message_id '{message_id}' already exists with different content. "
                        "Use edit_message_with_branching() to modify history safely.",
                        http_status=409,
                        role=role,
                    )
                return self._format_output([existing], branch_id=branch_id, role=role, idempotent_hit=True)

        branch_records = [
            r
            for r in bucket
            if r.user_id == user_id
            and r.project_id == project_id
            and r.session_id == session_id
            and r.branch_id == branch_id
        ]
        next_seq = (max((r.sequence_no for r in branch_records), default=0)) + 1

        record = StoredHistoryRecord(
            message_id=message_id,
            user_id=user_id,
            project_id=project_id,
            session_id=session_id,
            branch_id=branch_id,
            parent_branch_id=None,
            sequence_no=next_seq,
            sender=sender,
            sender_name=sender_name,
            visible_text=visible_text,
            effective_role=role,
            created_at=datetime.now(timezone.utc).isoformat(),
            signed_replay=signed_replay,
            cortex_thread_ref=cortex_thread_ref,
        )
        bucket.append(record)
        return self._format_output([record], branch_id=branch_id, role=role, idempotent_hit=False)

    def retrieve_messages(
        self,
        *,
        user_id: str,
        project_id: str,
        session_id: str,
        branch_id: str = "main",
        limit: int = 100,
    ) -> dict[str, Any]:
        role = self.backend.check_access(self.config, "SELECT")
        bucket = self._storage_bucket()
        matching = [
            r
            for r in bucket
            if r.user_id == user_id
            and r.project_id == project_id
            and r.session_id == session_id
            and r.branch_id == branch_id
            and r.effective_role == role
        ]
        matching.sort(key=lambda r: (r.sequence_no, r.created_at, r.message_id))
        if limit > 0:
            matching = matching[-limit:]
        return self._format_output(matching, branch_id=branch_id, role=role, idempotent_hit=False)

    def edit_message_with_branching(
        self,
        *,
        user_id: str,
        project_id: str,
        session_id: str,
        target_message_id: str,
        new_visible_text: str,
        source_branch_id: str = "main",
        new_branch_id: str | None = None,
    ) -> dict[str, Any]:
        """Edit a message without mutating signed protocol replay blocks in place (`REQ-08`).

        Creates a new branch (`new_branch_id`) copying history up to `target_message_id`,
        replaces the visible text on the edited turn, and invalidates any signed replay
        envelope on the modified turn while preserving earlier immutable signed blocks.
        The original `source_branch_id` remains 100% intact.
        """
        role = self.backend.check_access(self.config, "INSERT")
        bucket = self._storage_bucket()
        source_records = [
            r
            for r in bucket
            if r.user_id == user_id
            and r.project_id == project_id
            and r.session_id == session_id
            and r.branch_id == source_branch_id
            and r.effective_role == role
        ]
        source_records.sort(key=lambda r: r.sequence_no)

        target_idx = next(
            (idx for idx, r in enumerate(source_records) if r.message_id == target_message_id),
            None,
        )
        if target_idx is None:
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Message '{target_message_id}' not found in branch '{source_branch_id}'.",
                http_status=404,
                role=role,
            )

        fork_branch = new_branch_id or f"{source_branch_id}-branch-{target_message_id}"
        forked_records: list[StoredHistoryRecord] = []

        for idx in range(target_idx + 1):
            orig = source_records[idx]
            if idx < target_idx:
                cloned = copy.deepcopy(orig)
                cloned.branch_id = fork_branch
                cloned.parent_branch_id = source_branch_id
                forked_records.append(cloned)
            else:
                # Edited message: never keep a stale signature over modified content
                edited = StoredHistoryRecord(
                    message_id=f"{orig.message_id}@{fork_branch}",
                    user_id=orig.user_id,
                    project_id=orig.project_id,
                    session_id=orig.session_id,
                    branch_id=fork_branch,
                    parent_branch_id=source_branch_id,
                    sequence_no=orig.sequence_no,
                    sender=orig.sender,
                    sender_name=orig.sender_name,
                    visible_text=new_visible_text,
                    effective_role=role,
                    created_at=datetime.now(timezone.utc).isoformat(),
                    signed_replay=None,
                    cortex_thread_ref=orig.cortex_thread_ref,
                    replay_state_status="INVALIDATED_BY_EDIT",
                )
                forked_records.append(edited)

        bucket.extend(forked_records)
        return self._format_output(forked_records, branch_id=fork_branch, role=role, idempotent_hit=False)

    def delete_message_with_branching(
        self,
        *,
        user_id: str,
        project_id: str,
        session_id: str,
        target_message_id: str,
        source_branch_id: str = "main",
        new_branch_id: str | None = None,
    ) -> dict[str, Any]:
        """Delete a message by creating a clean branch truncated before `target_message_id`."""
        role = self.backend.check_access(self.config, "INSERT")
        bucket = self._storage_bucket()
        source_records = [
            r
            for r in bucket
            if r.user_id == user_id
            and r.project_id == project_id
            and r.session_id == session_id
            and r.branch_id == source_branch_id
            and r.effective_role == role
        ]
        source_records.sort(key=lambda r: r.sequence_no)
        target_idx = next(
            (idx for idx, r in enumerate(source_records) if r.message_id == target_message_id),
            None,
        )
        if target_idx is None:
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Message '{target_message_id}' not found in branch '{source_branch_id}'.",
                http_status=404,
                role=role,
            )

        fork_branch = new_branch_id or f"{source_branch_id}-del-{target_message_id}"
        forked_records: list[StoredHistoryRecord] = []
        for orig in source_records[:target_idx]:
            cloned = copy.deepcopy(orig)
            cloned.branch_id = fork_branch
            cloned.parent_branch_id = source_branch_id
            forked_records.append(cloned)

        bucket.extend(forked_records)
        return self._format_output(forked_records, branch_id=fork_branch, role=role, idempotent_hit=False)

    def _format_output(
        self,
        records: list[StoredHistoryRecord],
        *,
        branch_id: str,
        role: str,
        idempotent_hit: bool,
    ) -> dict[str, Any]:
        # Output port 1: `Messages` (visible UI chat messages ONLY — no raw signatures leaked into visible text)
        messages_port = [
            {
                "message_id": r.message_id,
                "sender": r.sender,
                "sender_name": r.sender_name,
                "text": r.visible_text,
                "session_id": r.session_id,
                "branch_id": r.branch_id,
                "sequence_no": r.sequence_no,
                "timestamp": r.created_at,
            }
            for r in records
        ]

        # Output port 2: `Table` (structured tabular rows)
        table_port = [
            {
                "MESSAGE_ID": r.message_id,
                "SEQUENCE_NO": r.sequence_no,
                "SENDER": r.sender,
                "SENDER_NAME": r.sender_name,
                "TEXT": r.visible_text,
                "SESSION_ID": r.session_id,
                "BRANCH_ID": r.branch_id,
                "HAS_SIGNED_REPLAY": bool(r.signed_replay and r.signed_replay.has_cryptographic_signature),
                "HAS_CORTEX_THREAD_REF": r.cortex_thread_ref is not None,
                "REPLAY_STATUS": r.replay_state_status,
            }
            for r in records
        ]

        # Separate protocol replay state & Cortex thread continuity state
        replay_envelopes = [
            {
                "message_id": r.message_id,
                "protocol": r.signed_replay.protocol,
                "thinking_signature": r.signed_replay.thinking_signature,
                "redacted_thinking": r.signed_replay.redacted_thinking,
                "encrypted_reasoning_content": r.signed_replay.encrypted_reasoning_content,
                "ordered_items": list(r.signed_replay.ordered_items),
                "tool_call_ids": list(r.signed_replay.tool_call_ids),
                "integrity_digest": r.signed_replay.integrity_digest,
            }
            for r in records
            if r.signed_replay is not None and r.replay_state_status == "VALID"
        ]

        cortex_threads = [
            {
                "message_id": r.message_id,
                "cortex_thread_id": r.cortex_thread_ref.cortex_thread_id,
                "cortex_parent_message_id": r.cortex_thread_ref.cortex_parent_message_id,
                "last_assistant_message_id": r.cortex_thread_ref.last_assistant_message_id,
                "agent_fqn": r.cortex_thread_ref.agent_fqn,
            }
            for r in records
            if r.cortex_thread_ref is not None
        ]

        return {
            "backend_type": self.config.backend_type.value,
            "table_fqn": self.config.table_fqn,
            "branch_id": branch_id,
            "effective_role": role,
            "idempotent_hit": idempotent_hit,
            "Messages": messages_port,
            "Table": table_port,
            "signed_replay_envelopes": replay_envelopes,
            "cortex_thread_references": cortex_threads,
        }
