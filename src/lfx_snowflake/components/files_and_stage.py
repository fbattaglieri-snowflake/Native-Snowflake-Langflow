"""Snowflake Files & Stage Components for Langflow (`REQ-10`, `[G-A3]`).

Implements:
- `SnowflakeFileSystemComponent` (`File System`): select existing stage or governed
  `CREATE STAGE` in the chosen `DB.SCHEMA` location, with backend sub-path validation
  even in `Tool Mode`.
- `SnowflakeReadFileComponent` (`Read File`): read files from authorized Snowflake stages,
  preserving `Select files / My Files`, `Advanced Parser`, and `Raw Content` without
  silently substituting `Advanced Parser` with Snowflake `AI_*` functions.
- `SnowflakeWriteFileComponent` (`Write File`): write to internal or external Snowflake
  stages with credential-free `@DB.SCHEMA.STAGE/path` outputs, explicit Internal vs
  External stage mechanics, safe atomic `Append` semantics (never silent overwrite,
  never corrupting JSON via raw text concatenation), and mandatory transfer completion
  verification before returning success.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import posixpath
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Mapping, Protocol

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
EXCLUDED_UPLOAD_NAMES = frozenset({".git", "__pycache__", ".DS_Store", ".env", ".venv"})
FORBIDDEN_URI_TOKENS = ("aws_key_id", "aws_secret_key", "sas_token", "presigned", "signature=", "token=")


class ErrorCode(str, Enum):
    AUTHZ_ROLE_DENIED = "AUTHZ_ROLE_DENIED"
    OBJECT_NOT_FOUND_OR_UNAUTHORIZED = "OBJECT_NOT_FOUND_OR_UNAUTHORIZED"
    UNSUPPORTED_CAPABILITY = "UNSUPPORTED_CAPABILITY"
    TECHNICAL_EXECUTION_ERROR = "TECHNICAL_EXECUTION_ERROR"


class SnowflakeComponentError(Exception):
    """Structured error carrying the 4-code governance taxonomy and HTTP status."""

    def __init__(
        self,
        code: ErrorCode | str,
        message: str,
        *,
        http_status: int = 400,
        role: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = ErrorCode(code) if not isinstance(code, ErrorCode) else code
        self.http_status = http_status
        self.role = role
        self.details = details or {}
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.code.value,
            "http_status": self.http_status,
            "message": str(self),
            "role": self.role,
            "details": self.details,
        }


def raise_role_denied(role: str, operation: str, resource: str, audit_sink: Callable[[dict[str, Any]], None] | None = None) -> None:
    msg = f"Il ruolo attivo ({role}) non consente questa operazione: {operation} su {resource}"
    if audit_sink is not None:
        audit_sink(
            {
                "event_type": "AUTHZ_ROLE_DENIED",
                "effective_role": role,
                "operation": operation,
                "resource": resource,
                "status": "DENIED",
                "http_status": 403,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
    raise SnowflakeComponentError(
        ErrorCode.AUTHZ_ROLE_DENIED,
        msg,
        http_status=403,
        role=role,
        details={"operation": operation, "resource": resource},
    )


def validate_snowflake_identifier(value: str, field_name: str) -> str:
    cleaned = (value or "").strip()
    if not IDENTIFIER_RE.match(cleaned):
        raise SnowflakeComponentError(
            ErrorCode.TECHNICAL_EXECUTION_ERROR,
            f"Invalid Snowflake identifier for {field_name}: {value!r}",
            http_status=400,
        )
    return cleaned.upper()


def normalize_and_validate_subpath(
    workspace_subpath: str | None,
    requested_path: str | None = None,
) -> str:
    """Validate and normalize stage sub-paths in backend (including Tool Mode).

    Blocks path traversal (`..`), absolute escapes, null bytes, backslash injection,
    and credential parameters. Enforces confinement under `workspace_subpath` when set.
    """
    base_raw = (workspace_subpath or "").strip()
    target_raw = (requested_path or "").strip()

    for label, candidate in (("workspace_subpath", base_raw), ("requested_path", target_raw)):
        if "\x00" in candidate or "\\" in candidate:
            raise SnowflakeComponentError(
                ErrorCode.AUTHZ_ROLE_DENIED,
                f"Invalid characters in {label}: path traversal or shell escape blocked.",
                http_status=403,
            )
        segments = [seg for seg in candidate.split("/") if seg]
        if any(seg in (".", "..") for seg in segments):
            raise SnowflakeComponentError(
                ErrorCode.AUTHZ_ROLE_DENIED,
                f"Path traversal ('..') is forbidden in {label}.",
                http_status=403,
            )
        if candidate.startswith("/"):
            raise SnowflakeComponentError(
                ErrorCode.AUTHZ_ROLE_DENIED,
                f"Absolute filesystem paths are forbidden in {label}.",
                http_status=403,
            )

    base_norm = posixpath.normpath(base_raw) if base_raw else ""
    if base_norm in (".", "/"):
        base_norm = ""

    if not target_raw:
        return base_norm

    target_norm = posixpath.normpath(target_raw).lstrip("/")
    if target_norm == ".":
        target_norm = ""

    if base_norm:
        if target_norm == base_norm or target_norm.startswith(base_norm + "/"):
            combined = target_norm
        else:
            combined = f"{base_norm}/{target_norm}" if target_norm else base_norm
        if not (combined == base_norm or combined.startswith(base_norm + "/")):
            raise SnowflakeComponentError(
                ErrorCode.AUTHZ_ROLE_DENIED,
                f"Path '{requested_path}' escapes configured workspace_subpath '{base_norm}'.",
                http_status=403,
            )
        return combined
    return target_norm


class StageType(str, Enum):
    INTERNAL = "INTERNAL"
    EXTERNAL = "EXTERNAL"


@dataclass(frozen=True)
class StageLocation:
    database: str
    schema: str
    stage_name: str
    stage_type: StageType = StageType.INTERNAL
    workspace_subpath: str = ""
    storage_integration: str | None = None
    external_url: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "database", validate_snowflake_identifier(self.database, "database"))
        object.__setattr__(self, "schema", validate_snowflake_identifier(self.schema, "schema"))
        object.__setattr__(self, "stage_name", validate_snowflake_identifier(self.stage_name, "stage_name"))
        norm_sub = normalize_and_validate_subpath(self.workspace_subpath)
        object.__setattr__(self, "workspace_subpath", norm_sub)
        if self.stage_type == StageType.EXTERNAL:
            if not self.storage_integration or not self.external_url:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    "External stages require both 'storage_integration' and 'external_url'.",
                    http_status=400,
                )
            object.__setattr__(
                self,
                "storage_integration",
                validate_snowflake_identifier(self.storage_integration, "storage_integration"),
            )

    @property
    def fqn(self) -> str:
        return f"{self.database}.{self.schema}.{self.stage_name}"

    def build_stage_uri(self, relative_path: str | None = None) -> str:
        resolved = normalize_and_validate_subpath(self.workspace_subpath, relative_path)
        uri = f"@{self.fqn}/{resolved}" if resolved else f"@{self.fqn}"
        lower_uri = uri.lower()
        if any(tok in lower_uri for tok in FORBIDDEN_URI_TOKENS) or "://" in lower_uri:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                "Stage URI must be a credential-free '@DB.SCHEMA.STAGE/path' reference.",
                http_status=400,
            )
        return uri


@dataclass
class StageObjectRecord:
    stage_uri: str
    content: bytes
    sha256: str
    size_bytes: int
    updated_at: str
    etag: str
    metadata: dict[str, Any] = field(default_factory=dict)


class StageBackendAdapter(Protocol):
    """Protocol for interacting with Snowflake internal and external stages."""

    def get_current_role(self) -> str: ...

    def has_privilege(self, role: str, privilege: str, object_fqn: str) -> bool: ...

    def stage_exists(self, stage_fqn: str) -> bool: ...

    def get_stage_type(self, stage_fqn: str) -> StageType: ...

    def create_stage_ddl(self, location: StageLocation, *, role: str, confirmed: bool) -> dict[str, Any]: ...

    def list_stage_files(self, location: StageLocation, prefix: str = "") -> list[dict[str, Any]]: ...

    def read_stage_bytes(self, stage_uri: str) -> StageObjectRecord: ...

    def put_internal_stage_stream(
        self,
        stage_uri: str,
        data: bytes,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]: ...

    def write_external_stage_object(
        self,
        stage_uri: str,
        data: bytes,
        *,
        overwrite: bool = False,
        expected_etag: str | None = None,
    ) -> dict[str, Any]: ...


class InMemoryStageBackend:
    """Deterministic in-memory stage backend for contract/unit testing and dry-run verification."""

    def __init__(
        self,
        *,
        active_role: str = "ROLE_A",
        role_privileges: Mapping[str, set[tuple[str, str]]] | None = None,
        audit_sink: Callable[[dict[str, Any]], None] | None = None,
        external_stage_supports_atomic_write: bool = True,
        simulate_incomplete_transfer: bool = False,
    ) -> None:
        self.active_role = active_role
        self.role_privileges: dict[str, set[tuple[str, str]]] = {
            k: set(v) for k, v in (role_privileges or {}).items()
        }
        self.audit_log: list[dict[str, Any]] = []
        self._external_audit_sink = audit_sink
        self.stages: dict[str, StageLocation] = {}
        self.objects: dict[str, StageObjectRecord] = {}
        self.executed_sql: list[str] = []
        self.external_stage_supports_atomic_write = external_stage_supports_atomic_write
        self.simulate_incomplete_transfer = simulate_incomplete_transfer

    def record_audit(self, event: dict[str, Any]) -> None:
        self.audit_log.append(event)
        if self._external_audit_sink is not None:
            self._external_audit_sink(event)

    def get_current_role(self) -> str:
        return self.active_role

    def has_privilege(self, role: str, privilege: str, object_fqn: str) -> bool:
        allowed = self.role_privileges.get(role, set())
        return (privilege.upper(), object_fqn.upper()) in allowed or (privilege.upper(), "*") in allowed

    def stage_exists(self, stage_fqn: str) -> bool:
        return stage_fqn.upper() in self.stages

    def get_stage_type(self, stage_fqn: str) -> StageType:
        loc = self.stages.get(stage_fqn.upper())
        if loc is None:
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Stage {stage_fqn} does not exist or is not authorized.",
                http_status=404,
            )
        return loc.stage_type

    def create_stage_ddl(self, location: StageLocation, *, role: str, confirmed: bool) -> dict[str, Any]:
        schema_fqn = f"{location.database}.{location.schema}"
        if not self.has_privilege(role, "CREATE STAGE", schema_fqn):
            raise_role_denied(role, "CREATE STAGE", schema_fqn, self.record_audit)

        if self.stage_exists(location.fqn):
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"Stage {location.fqn} already exists. Silent 'CREATE OR REPLACE STAGE' is prohibited.",
                http_status=409,
            )

        if location.stage_type == StageType.INTERNAL:
            ddl = f"CREATE STAGE {location.fqn} DIRECTORY = (ENABLE = TRUE)"
        else:
            ddl = (
                f"CREATE STAGE {location.fqn} "
                f"URL = '{location.external_url}' "
                f"STORAGE_INTEGRATION = {location.storage_integration} "
                f"DIRECTORY = (ENABLE = TRUE)"
            )

        if not confirmed:
            return {
                "status": "DRY_RUN_PREVIEW",
                "ddl": ddl,
                "stage_fqn": location.fqn,
                "stage_type": location.stage_type.value,
                "effective_role": role,
            }

        self.executed_sql.append(ddl)
        self.stages[location.fqn] = location
        # Grant owner privileges to creating role
        self.role_privileges.setdefault(role, set()).update(
            {
                ("USAGE", location.fqn),
                ("READ", location.fqn),
                ("WRITE", location.fqn),
            }
        )
        self.record_audit(
            {
                "event_type": "CREATE_STAGE",
                "effective_role": role,
                "resource": location.fqn,
                "stage_type": location.stage_type.value,
                "ddl": ddl,
                "status": "SUCCESS",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return {
            "status": "CREATED",
            "ddl": ddl,
            "stage_fqn": location.fqn,
            "stage_type": location.stage_type.value,
            "owner_role": role,
        }

    def list_stage_files(self, location: StageLocation, prefix: str = "") -> list[dict[str, Any]]:
        base_uri = location.build_stage_uri(prefix)
        results: list[dict[str, Any]] = []
        for uri, rec in sorted(self.objects.items()):
            if uri == base_uri or uri.startswith(base_uri.rstrip("/") + "/"):
                results.append(
                    {
                        "stage_uri": uri,
                        "size_bytes": rec.size_bytes,
                        "sha256": rec.sha256,
                        "etag": rec.etag,
                        "updated_at": rec.updated_at,
                    }
                )
        return results

    def read_stage_bytes(self, stage_uri: str) -> StageObjectRecord:
        rec = self.objects.get(stage_uri)
        if rec is None:
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"File not found on stage: {stage_uri}",
                http_status=404,
            )
        return rec

    def put_internal_stage_stream(
        self,
        stage_uri: str,
        data: bytes,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        if stage_uri in self.objects and not overwrite:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"Target file {stage_uri} already exists and overwrite=False.",
                http_status=409,
            )
        digest = hashlib.sha256(data).hexdigest()
        transferred = max(0, len(data) - 5) if self.simulate_incomplete_transfer else len(data)
        etag = f"etag-int-{digest[:16]}"
        now = datetime.now(timezone.utc).isoformat()
        if not self.simulate_incomplete_transfer:
            self.objects[stage_uri] = StageObjectRecord(
                stage_uri=stage_uri,
                content=data,
                sha256=digest,
                size_bytes=len(data),
                updated_at=now,
                etag=etag,
            )
        self.executed_sql.append(
            f"PUT stream -> {stage_uri} AUTO_COMPRESS=FALSE OVERWRITE={'TRUE' if overwrite else 'FALSE'}"
        )
        return {
            "status": "UPLOADED" if not self.simulate_incomplete_transfer else "PARTIAL",
            "stage_uri": stage_uri,
            "transferred_bytes": transferred,
            "expected_bytes": len(data),
            "sha256": digest if not self.simulate_incomplete_transfer else "mismatch",
            "etag": etag,
        }

    def write_external_stage_object(
        self,
        stage_uri: str,
        data: bytes,
        *,
        overwrite: bool = False,
        expected_etag: str | None = None,
    ) -> dict[str, Any]:
        if not self.external_stage_supports_atomic_write:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                "External stage connector does not have atomic write capability enabled.",
                http_status=422,
            )
        existing = self.objects.get(stage_uri)
        if existing is not None:
            if not overwrite:
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    f"External stage file {stage_uri} already exists and overwrite=False.",
                    http_status=409,
                )
            if expected_etag is not None and existing.etag != expected_etag:
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    f"Concurrent modification detected on {stage_uri}: expected etag {expected_etag}, found {existing.etag}.",
                    http_status=409,
                )
        digest = hashlib.sha256(data).hexdigest()
        transferred = max(0, len(data) - 5) if self.simulate_incomplete_transfer else len(data)
        etag = f"etag-ext-{digest[:16]}"
        now = datetime.now(timezone.utc).isoformat()
        if not self.simulate_incomplete_transfer:
            self.objects[stage_uri] = StageObjectRecord(
                stage_uri=stage_uri,
                content=data,
                sha256=digest,
                size_bytes=len(data),
                updated_at=now,
                etag=etag,
            )
        return {
            "status": "COMMITTED" if not self.simulate_incomplete_transfer else "INCOMPLETE",
            "stage_uri": stage_uri,
            "transferred_bytes": transferred,
            "expected_bytes": len(data),
            "sha256": digest if not self.simulate_incomplete_transfer else "mismatch",
            "etag": etag,
        }


class SnowflakeFileSystemComponent:
    """Langflow `File System` component extended for Snowflake Stages (`REQ-10`).

    Distinguishes:
    - Using an existing stage (`READ` / `USAGE` privilege check only, no `CREATE STAGE` required)
    - Creating a new stage (`CREATE STAGE` privilege check on `DB.SCHEMA`, preview/confirmation,
      ownership to active role, no silent `OR REPLACE`).
    Enforces `workspace_subpath` confinement in backend both in interactive UI and `Tool Mode`.
    """

    display_name = "File System"
    category = "Files & Knowledge"
    supported_operations = ("create_stage", "list_files", "resolve_stage_uri", "stat_file")

    def __init__(
        self,
        backend: StageBackendAdapter,
        location: StageLocation,
        *,
        tool_mode: bool = False,
        audit_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.backend = backend
        self.location = location
        self.tool_mode = tool_mode
        self.audit_sink = audit_sink or getattr(backend, "record_audit", None)

    def create_stage(self, *, confirmed: bool = False) -> dict[str, Any]:
        role = self.backend.get_current_role()
        return self.backend.create_stage_ddl(self.location, role=role, confirmed=confirmed)

    def resolve_stage_uri(self, relative_path: str = "") -> str:
        role = self.backend.get_current_role()
        if not self.backend.stage_exists(self.location.fqn):
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Stage {self.location.fqn} not found.",
                http_status=404,
                role=role,
            )
        if not (
            self.backend.has_privilege(role, "READ", self.location.fqn)
            or self.backend.has_privilege(role, "USAGE", self.location.fqn)
        ):
            raise_role_denied(role, "READ/USAGE STAGE", self.location.fqn, self.audit_sink)
        # Validate sub-path even when invoked as a tool by an agent
        return self.location.build_stage_uri(relative_path)

    def list_files(self, relative_prefix: str = "") -> list[dict[str, Any]]:
        role = self.backend.get_current_role()
        if not self.backend.stage_exists(self.location.fqn):
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Stage {self.location.fqn} not found.",
                http_status=404,
                role=role,
            )
        if not self.backend.has_privilege(role, "READ", self.location.fqn):
            raise_role_denied(role, "LIST/READ STAGE", self.location.fqn, self.audit_sink)
        # Validate relative_prefix confinement
        _ = self.location.build_stage_uri(relative_prefix)
        return self.backend.list_stage_files(self.location, relative_prefix)


class SnowflakeReadFileComponent:
    """Langflow `Read File` component backed by Snowflake Stage (`REQ-10`).

    Preserves `Select files / My Files`, `Advanced Parser`, and `Raw Content` output port.
    Never automatically substitutes `Advanced Parser` with Snowflake `AI_*` functions.
    """

    display_name = "Read File"
    category = "Files & Knowledge"
    ports = ("raw_content", "parsed_data", "stage_uri")

    def __init__(
        self,
        backend: StageBackendAdapter,
        location: StageLocation,
        *,
        advanced_parser: bool = False,
        max_bytes: int = 100 * 1024 * 1024,
        audit_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.backend = backend
        self.location = location
        self.advanced_parser = advanced_parser
        self.max_bytes = max_bytes
        self.audit_sink = audit_sink or getattr(backend, "record_audit", None)

    def read_file(self, selected_file: str) -> dict[str, Any]:
        role = self.backend.get_current_role()
        if not self.backend.stage_exists(self.location.fqn):
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Stage {self.location.fqn} not found.",
                http_status=404,
                role=role,
            )
        if not self.backend.has_privilege(role, "READ", self.location.fqn):
            raise_role_denied(role, "READ STAGE", self.location.fqn, self.audit_sink)

        stage_uri = self.location.build_stage_uri(selected_file)
        record = self.backend.read_stage_bytes(stage_uri)
        if record.size_bytes > self.max_bytes:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"File {stage_uri} ({record.size_bytes} bytes) exceeds runtime read limit ({self.max_bytes} bytes).",
                http_status=413,
                role=role,
            )

        raw_text = record.content.decode("utf-8", errors="replace")
        parsed_payload: Any = None
        if self.advanced_parser:
            # Local deterministic structural parser — NEVER calls AI_PARSE_DOCUMENT or AI_EXTRACT implicitly
            if selected_file.lower().endswith(".json"):
                parsed_payload = json.loads(raw_text)
            elif selected_file.lower().endswith(".csv"):
                reader = csv.DictReader(io.StringIO(raw_text))
                parsed_payload = list(reader)
            else:
                parsed_payload = {"lines": raw_text.splitlines(), "parser": "langflow_local_advanced_parser"}

        return {
            "stage_uri": stage_uri,
            "raw_content": raw_text,
            "parsed_data": parsed_payload,
            "used_ai_functions": False,
            "advanced_parser_enabled": self.advanced_parser,
            "size_bytes": record.size_bytes,
            "sha256": record.sha256,
            "effective_role": role,
        }

    def upload_local_to_stage(
        self,
        file_name: str,
        content: bytes | str,
        *,
        explicit_upload_action: bool = False,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        if not explicit_upload_action:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                "Local file upload to stage requires an explicit user upload action.",
                http_status=400,
            )
        parts = [p for p in (file_name or "").split("/") if p]
        if any(p in EXCLUDED_UPLOAD_NAMES for p in parts):
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"File or folder '{file_name}' is excluded from stage upload ({sorted(EXCLUDED_UPLOAD_NAMES)}).",
                http_status=400,
            )
        writer = SnowflakeWriteFileComponent(
            self.backend,
            self.location,
            audit_sink=self.audit_sink,
        )
        return writer.write_file(
            file_content=content,
            file_name=file_name,
            file_format=file_name.rsplit(".", 1)[-1].lower() if "." in file_name else "txt",
            append=False,
            overwrite=overwrite,
        )


class SnowflakeWriteFileComponent:
    """Langflow `Write File` component backed by Snowflake Stage (`REQ-10`, `[G-A3]`).

    Preserves `File Content`, `File Name`, `Append`, `File Format`, and `File Path` output.
    Key guarantees (`[G-A3]`):
    1. Distinguishes Internal Stages (`PUT` stream upload) vs External Stages
       (external stage object connector, since SQL `PUT` does not support external stages).
    2. Never translates `Append=True` into a silent overwrite.
    3. Never performs raw textual append on `.json` documents (which would corrupt valid JSON).
       Supports `append_json_mode='array_merge'` for JSON arrays or raises `UNSUPPORTED_CAPABILITY`.
    4. Supports controlled atomic `Append` (`atomic_rewrite` with ETag check or `incremental_parts`)
       for line/text formats (`txt`, `md`, `csv`, `jsonl`) and blocks unsupported stage/format pairs.
    5. Verifies actual transfer completion (`transferred_bytes == expected_bytes`, `sha256`, and
       presence in `LIST @stage`) before returning the credential-free `@DB.SCHEMA.STAGE/path` URI.
    """

    display_name = "Write File"
    category = "Files & Knowledge"
    supported_formats = ("txt", "md", "csv", "jsonl", "json", "parquet")
    append_capable_text_formats = ("txt", "md", "csv", "jsonl")

    def __init__(
        self,
        backend: StageBackendAdapter,
        location: StageLocation,
        *,
        audit_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.backend = backend
        self.location = location
        self.audit_sink = audit_sink or getattr(backend, "record_audit", None)

    def write_file(
        self,
        *,
        file_content: str | bytes | list[Any] | dict[str, Any],
        file_name: str,
        file_format: str = "txt",
        append: bool = False,
        append_strategy: str = "atomic_rewrite",
        append_json_mode: str = "forbid_raw",
        overwrite: bool = False,
    ) -> dict[str, Any]:
        role = self.backend.get_current_role()
        if not self.backend.stage_exists(self.location.fqn):
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Stage {self.location.fqn} not found.",
                http_status=404,
                role=role,
            )
        if not self.backend.has_privilege(role, "WRITE", self.location.fqn):
            raise_role_denied(role, "WRITE STAGE", self.location.fqn, self.audit_sink)

        fmt = (file_format or "txt").strip().lower().lstrip(".")
        if fmt not in self.supported_formats:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Unsupported file_format '{fmt}'. Supported formats: {self.supported_formats}",
                http_status=400,
                role=role,
            )

        stage_type = self.backend.get_stage_type(self.location.fqn)
        stage_uri = self.location.build_stage_uri(file_name)
        new_bytes = self._serialize_content(file_content, fmt)

        existing_record: StageObjectRecord | None = None
        try:
            existing_record = self.backend.read_stage_bytes(stage_uri)
        except SnowflakeComponentError as exc:
            if exc.code != ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED:
                raise

        target_uri = stage_uri
        final_bytes = new_bytes
        effective_overwrite = overwrite
        expected_etag: str | None = None
        operation_mode = "WRITE_NEW"

        if append:
            if fmt == "parquet":
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    "Append is disabled for binary format 'parquet'. Use incremental part files or a table.",
                    http_status=422,
                    role=role,
                )

            if append_strategy not in ("atomic_rewrite", "incremental_parts"):
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    f"Unsupported append_strategy '{append_strategy}'. Use 'atomic_rewrite' or 'incremental_parts'.",
                    http_status=400,
                    role=role,
                )

            if append_strategy == "incremental_parts":
                if fmt == "json":
                    raise SnowflakeComponentError(
                        ErrorCode.UNSUPPORTED_CAPABILITY,
                        "Incremental part append is not valid for single-document JSON; use 'jsonl' or 'atomic_rewrite' with array_merge.",
                        http_status=422,
                        role=role,
                    )
                existing_parts = self.backend.list_stage_files(self.location, file_name)
                part_index = len(existing_parts) + 1
                target_uri = f"{stage_uri}.part-{part_index:04d}.{fmt}"
                final_bytes = new_bytes
                effective_overwrite = False
                operation_mode = "APPEND_INCREMENTAL_PART"
            else:
                # atomic_rewrite
                if existing_record is None:
                    final_bytes = new_bytes
                    effective_overwrite = False
                    operation_mode = "APPEND_INITIAL_CREATE"
                else:
                    expected_etag = existing_record.etag
                    final_bytes = self._merge_for_atomic_append(
                        existing_record.content,
                        new_bytes,
                        fmt=fmt,
                        append_json_mode=append_json_mode,
                    )
                    effective_overwrite = True
                    operation_mode = "APPEND_ATOMIC_REWRITE"
        else:
            if existing_record is not None and not overwrite:
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    f"File {stage_uri} already exists. Set append=True or overwrite=True explicitly; silent overwrite is prohibited.",
                    http_status=409,
                    role=role,
                )
            if existing_record is not None and overwrite:
                operation_mode = "EXPLICIT_OVERWRITE"

        # Execute stage-type-specific transfer:
        # SQL PUT only works on INTERNAL stages; EXTERNAL stages use dedicated external object writer.
        if stage_type == StageType.INTERNAL:
            transfer_receipt = self.backend.put_internal_stage_stream(
                target_uri,
                final_bytes,
                overwrite=effective_overwrite,
            )
            transport_method = "INTERNAL_STAGE_PUT_STREAM"
        else:
            transfer_receipt = self.backend.write_external_stage_object(
                target_uri,
                final_bytes,
                overwrite=effective_overwrite,
                expected_etag=expected_etag,
            )
            transport_method = "EXTERNAL_STAGE_INTEGRATION_WRITER"

        # Mandatory transfer verification before returning success
        self._verify_transfer_completion(
            target_uri=target_uri,
            expected_bytes=final_bytes,
            receipt=transfer_receipt,
            role=role,
        )

        if self.audit_sink is not None:
            self.audit_sink(
                {
                    "event_type": "WRITE_STAGE_FILE",
                    "effective_role": role,
                    "resource": target_uri,
                    "stage_type": stage_type.value,
                    "operation_mode": operation_mode,
                    "transport_method": transport_method,
                    "size_bytes": len(final_bytes),
                    "sha256": transfer_receipt["sha256"],
                    "status": "SUCCESS",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )

        return {
            "file_path": target_uri,
            "stage_type": stage_type.value,
            "transport_method": transport_method,
            "operation_mode": operation_mode,
            "size_bytes": len(final_bytes),
            "sha256": transfer_receipt["sha256"],
            "etag": transfer_receipt.get("etag"),
            "transfer_verified": True,
            "effective_role": role,
        }

    def _serialize_content(
        self,
        content: str | bytes | list[Any] | dict[str, Any],
        fmt: str,
    ) -> bytes:
        if isinstance(content, bytes):
            if fmt == "json":
                try:
                    json.loads(content.decode("utf-8"))
                except Exception as exc:
                    raise SnowflakeComponentError(
                        ErrorCode.TECHNICAL_EXECUTION_ERROR,
                        f"Invalid JSON payload for Write File: {exc}",
                        http_status=400,
                    ) from exc
            return content

        if fmt == "json":
            if isinstance(content, str):
                try:
                    parsed = json.loads(content)
                except Exception as exc:
                    raise SnowflakeComponentError(
                        ErrorCode.TECHNICAL_EXECUTION_ERROR,
                        f"Invalid JSON string for Write File: {exc}",
                        http_status=400,
                    ) from exc
                return json.dumps(parsed, ensure_ascii=False, indent=2).encode("utf-8")
            return json.dumps(content, ensure_ascii=False, indent=2).encode("utf-8")

        if fmt == "jsonl":
            if isinstance(content, list):
                lines = [json.dumps(item, ensure_ascii=False) for item in content]
                return ("\n".join(lines) + "\n").encode("utf-8")
            if isinstance(content, dict):
                return (json.dumps(content, ensure_ascii=False) + "\n").encode("utf-8")
            text = str(content).strip()
            for line in text.splitlines():
                if line.strip():
                    json.loads(line)
            return (text + "\n").encode("utf-8")

        if isinstance(content, (dict, list)):
            return json.dumps(content, ensure_ascii=False).encode("utf-8")
        return str(content).encode("utf-8")

    def _merge_for_atomic_append(
        self,
        existing_bytes: bytes,
        new_bytes: bytes,
        *,
        fmt: str,
        append_json_mode: str,
    ) -> bytes:
        if fmt == "json":
            if append_json_mode != "array_merge":
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    "Raw text append on 'json' format is blocked to prevent JSON corruption. "
                    "Use format='jsonl' or set append_json_mode='array_merge' on JSON array documents.",
                    http_status=422,
                )
            existing_doc = json.loads(existing_bytes.decode("utf-8"))
            new_doc = json.loads(new_bytes.decode("utf-8"))
            if not isinstance(existing_doc, list):
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    "JSON atomic array_merge requires existing JSON document root to be a list.",
                    http_status=422,
                )
            merged_list = list(existing_doc)
            if isinstance(new_doc, list):
                merged_list.extend(new_doc)
            else:
                merged_list.append(new_doc)
            return json.dumps(merged_list, ensure_ascii=False, indent=2).encode("utf-8")

        existing_text = existing_bytes.decode("utf-8")
        new_text = new_bytes.decode("utf-8")

        if fmt == "csv":
            existing_lines = [ln for ln in existing_text.splitlines() if ln.strip()]
            new_lines = [ln for ln in new_text.splitlines() if ln.strip()]
            if existing_lines and new_lines and existing_lines[0].strip() == new_lines[0].strip():
                # Strip duplicate header row when appending CSV batches
                new_lines = new_lines[1:]
            combined = existing_lines + new_lines
            return ("\n".join(combined) + ("\n" if combined else "")).encode("utf-8")

        if fmt == "jsonl":
            sep = "" if existing_text.endswith("\n") or not existing_text else "\n"
            combined_text = existing_text + sep + new_text
            if not combined_text.endswith("\n"):
                combined_text += "\n"
            return combined_text.encode("utf-8")

        # txt / md
        sep = "" if existing_text.endswith("\n") or not existing_text else "\n"
        return (existing_text + sep + new_text).encode("utf-8")

    def _verify_transfer_completion(
        self,
        *,
        target_uri: str,
        expected_bytes: bytes,
        receipt: Mapping[str, Any],
        role: str,
    ) -> None:
        expected_sha256 = hashlib.sha256(expected_bytes).hexdigest()
        status = str(receipt.get("status", "")).upper()
        transferred = int(receipt.get("transferred_bytes", -1))
        receipt_sha = str(receipt.get("sha256", ""))

        if status not in ("UPLOADED", "COMMITTED") or transferred != len(expected_bytes) or receipt_sha != expected_sha256:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"Stage file transfer verification failed for {target_uri}: "
                f"status={status}, transferred={transferred}/{len(expected_bytes)}.",
                http_status=502,
                role=role,
            )

        # Confirm presence and digest on stage directory listing / read-back
        stored = self.backend.read_stage_bytes(target_uri)
        if stored.size_bytes != len(expected_bytes) or stored.sha256 != expected_sha256:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"Post-write stage read-back verification failed for {target_uri}.",
                http_status=502,
                role=role,
            )
