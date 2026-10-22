"""Governed Creation of Snowflake and Snowflake Postgres Objects (ADR-005, REQ-16).

Implements `SnowflakeObjectProvisioner` with:
- Strict SQL identifier and hierarchy validation (`Account -> Database -> Schema -> Object`
  and `PostgresInstance -> Database -> Schema -> Table`)
- Prohibition of silent `OR REPLACE` overwrites and inline cloud credentials
- Pre-execution zero-credit privilege verification (`EXPLAIN_PRIVILEGES(..., missing_only => true)`)
- Dry-run preview (`ProvisioningPreview`) with FQN destination, active owner role,
  required privileges, and billable impact
- Mandatory explicit user confirmation before DDL execution
- Synchronized durable audit logging (`LANGFLOW_AUDIT_LOG`) for both successes and denials
"""

from __future__ import annotations

import re
from typing import Any, Callable

from lfx_snowflake.governance.authz_service import SnowflakeAuthzService
from lfx_snowflake.governance.models import (
    ObjectFamily,
    OperationType,
    ProvisioningExecutionResult,
    ProvisioningPreview,
    SnowflakeErrorCode,
    SnowflakeGovernanceError,
    SubsystemBoundary,
)


class SnowflakeObjectProvisioner:
    """Governed object provisioner for Snowflake and Snowflake Postgres (:5432)."""

    _IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
    _VECTOR_TYPE_RE = re.compile(
        r"^VECTOR\s*\(\s*(FLOAT|INT)\s*,\s*(\d+)\s*\)$",
        re.IGNORECASE,
    )
    _TARGET_LAG_RE = re.compile(
        r"^(\d+\s+(seconds?|minutes?|hours?|days?)|DOWNSTREAM)$",
        re.IGNORECASE,
    )
    _ALLOWED_EXTERNAL_STAGE_SCHEMES = ("s3://", "gcs://", "azure://", "s3compat://")
    _VALID_PG_AUTH_AUTHORITIES = {"POSTGRES", "POSTGRES_OR_SNOWFLAKE"}

    def __init__(
        self,
        authz_service: SnowflakeAuthzService,
        ddl_executor: Callable[[str, str], str] | None = None,
        pg_ddl_executor: Callable[[str, str], str] | None = None,
    ) -> None:
        self.authz_service = authz_service
        self._ddl_executor = ddl_executor or (lambda _role, _sql: f"qid_{abs(hash(_sql)) % 1000000:06d}")
        self._pg_ddl_executor = pg_ddl_executor or (lambda _pg_role, _sql: "pg_tx_ok")

    # ------------------------------------------------------------------
    # Identifier, Hierarchy & Stage Sub-Path Validation
    # ------------------------------------------------------------------
    @classmethod
    def validate_identifier(cls, identifier: str, label: str = "identifier") -> str:
        """Validate a Snowflake/PostgreSQL identifier without allowing SQL injection or quotes."""
        cleaned = (identifier or "").strip()
        if not cls._IDENTIFIER_RE.match(cleaned):
            raise ValueError(f"Invalid {label} '{identifier}': must match {cls._IDENTIFIER_RE.pattern}")
        return cleaned.upper()

    @classmethod
    def validate_pg_identifier(cls, identifier: str, label: str = "pg_identifier") -> str:
        """Validate a PostgreSQL identifier preserving lowercase convention."""
        cleaned = (identifier or "").strip()
        if not cls._IDENTIFIER_RE.match(cleaned):
            raise ValueError(f"Invalid {label} '{identifier}': must match {cls._IDENTIFIER_RE.pattern}")
        return cleaned.lower()

    @classmethod
    def validate_stage_subpath(
        cls,
        *,
        database: str,
        schema: str,
        stage: str,
        subpath: str = "",
        active_role: str = "UNKNOWN",
    ) -> str:
        """Validate stage FQN and workspace sub-path (also enforced in Tool Mode)."""
        db_id = cls.validate_identifier(database, "database")
        sch_id = cls.validate_identifier(schema, "schema")
        stg_id = cls.validate_identifier(stage, "stage")

        raw_subpath = (subpath or "").strip()
        if not raw_subpath or raw_subpath == "/":
            return f"@{db_id}.{sch_id}.{stg_id}"

        normalized = raw_subpath.lstrip("/")
        if (
            ".." in normalized.split("/")
            or "\\" in normalized
            or ":" in normalized
            or "@" in normalized
            or any(ord(ch) < 32 for ch in normalized)
            or normalized.startswith("~")
        ):
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=active_role,
                operation=OperationType.READ_STAGE_FILE.value,
                reason=f"Unsafe or invalid stage sub-path '{subpath}'.",
            )
        for segment in normalized.split("/"):
            if not segment or segment in (".", ".."):
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=active_role,
                    operation=OperationType.READ_STAGE_FILE.value,
                    reason=f"Invalid stage path segment in '{subpath}'.",
                )
        return f"@{db_id}.{sch_id}.{stg_id}/{normalized}"

    @staticmethod
    def _ensure_no_or_replace(sql_text: str, active_role: str, operation: str) -> None:
        if re.search(r"\bOR\s+REPLACE\b", sql_text, flags=re.IGNORECASE):
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=active_role,
                operation=operation,
                reason="Silent 'OR REPLACE' destructive DDL is prohibited by governance policy (ADR-005).",
            )

    @classmethod
    def _validate_vector_column_spec(
        cls,
        col_type: str,
        active_role: str,
        operation: str,
        expected_embedding_dim: int | None = None,
    ) -> int | None:
        cleaned = col_type.strip().upper()
        if not cleaned.startswith("VECTOR"):
            return None
        match = cls._VECTOR_TYPE_RE.match(cleaned)
        if not match:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=active_role,
                operation=operation,
                reason=f"Invalid VECTOR syntax '{col_type}'. Expected VECTOR(FLOAT|INT, dim).",
            )
        dim = int(match.group(2))
        if dim < 1 or dim > 4096:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=active_role,
                operation=operation,
                reason=f"VECTOR dimension {dim} is out of bounds (Snowflake supports 1..4096).",
            )
        if expected_embedding_dim is not None and dim != expected_embedding_dim:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=active_role,
                operation=operation,
                reason=(
                    f"VECTOR dimension {dim} does not match selected embedding model "
                    f"dimension {expected_embedding_dim}."
                ),
            )
        return dim

    # ------------------------------------------------------------------
    # 1. Snowflake Postgres (Account-Level Instance vs Internal PG :5432)
    # ------------------------------------------------------------------
    def preview_postgres_instance(
        self,
        *,
        user_principal: str,
        instance_name: str,
        compute_family: str = "STANDARD_M",
        storage_size_gb: int = 10,
        authentication_authority: str = "POSTGRES_OR_SNOWFLAKE",
        network_policy: str | None = None,
    ) -> ProvisioningPreview:
        """Preview account-level `CREATE POSTGRES INSTANCE` (billable resource)."""
        ctx = self.authz_service.check_operation_permission(
            user_principal=user_principal,
            operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
            target_ref=f"ACCOUNT.POSTGRES_INSTANCE.{instance_name}",
            required_privilege="CREATE POSTGRES INSTANCE ON ACCOUNT",
            disclosable=True,
        )
        inst_id = self.validate_identifier(instance_name, "postgres_instance_name")
        family_id = self.validate_identifier(compute_family, "compute_family")
        auth_auth = authentication_authority.strip().upper()
        if auth_auth not in self._VALID_PG_AUTH_AUTHORITIES:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation="CREATE_POSTGRES_INSTANCE",
                reason=f"Unsupported AUTHENTICATION_AUTHORITY '{authentication_authority}'.",
            )
        if storage_size_gb < 10 or storage_size_gb > 65535:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation="CREATE_POSTGRES_INSTANCE",
                reason="STORAGE_SIZE_GB must be between 10 and 65535.",
            )

        ddl = (
            f"CREATE POSTGRES INSTANCE {inst_id} "
            f"COMPUTE_FAMILY = '{family_id}' "
            f"STORAGE_SIZE_GB = {int(storage_size_gb)} "
            f"AUTHENTICATION_AUTHORITY = {auth_auth}"
        )
        if network_policy:
            np_id = self.validate_identifier(network_policy, "network_policy")
            ddl += f" NETWORK_POLICY = '{np_id}'"

        self.authz_service.precheck_sql_privileges(
            user_principal=user_principal,
            sql_statement=ddl,
            operation="CREATE_POSTGRES_INSTANCE",
            target_ref=inst_id,
            disclosable=True,
        )
        return ProvisioningPreview(
            object_family=ObjectFamily.POSTGRES_INSTANCE,
            destination_fqn=inst_id,
            hierarchy_path=("ACCOUNT", inst_id),
            owner_role=ctx.active_role,
            required_privileges=("CREATE POSTGRES INSTANCE ON ACCOUNT",),
            ddl_statements=(ddl,),
            billable_impact=(
                f"Billable Snowflake Postgres compute ({family_id}) + "
                f"{int(storage_size_gb)} GB storage; credentials returned once and redacted from logs/UI."
            ),
            requires_confirmation=True,
            warnings=(
                "Requires network policy with MODE = POSTGRES_INGRESS for port :5432 access.",
                "Never log or display access_roles passwords in plaintext.",
            ),
            subsystem=SubsystemBoundary.SNOWFLAKE_SQL,
        )

    def preview_postgres_internal_vector_table(
        self,
        *,
        user_principal: str,
        pg_role: str,
        instance_name: str,
        pg_database: str,
        pg_schema: str,
        pg_table: str,
        vector_dimension: int,
        sslmode: str = "verify-ca",
        connect_timeout: int = 10,
    ) -> ProvisioningPreview:
        """Preview DDL inside an existing Snowflake Postgres instance (:5432 with pgvector)."""
        pg_auth = self.authz_service.authorize_postgres_operation(
            user_principal=user_principal,
            pg_role=pg_role,
            required_pg_privilege="CREATE",
            sslmode=sslmode,
            connect_timeout=connect_timeout,
            target_ref=f"{instance_name}/{pg_database}.{pg_schema}.{pg_table}",
        )
        inst_id = self.validate_identifier(instance_name, "instance_name")
        db_id = self.validate_pg_identifier(pg_database, "pg_database")
        sch_id = self.validate_pg_identifier(pg_schema, "pg_schema")
        tbl_id = self.validate_pg_identifier(pg_table, "pg_table")

        if vector_dimension < 1 or vector_dimension > 4096:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=pg_auth["snowflake_active_role"],
                operation="CREATE_PGVECTOR_TABLE",
                reason=f"Invalid pgvector dimension {vector_dimension} (expected 1..4096).",
                subsystem=SubsystemBoundary.SNOWFLAKE_POSTGRES,
            )

        statements = (
            "CREATE EXTENSION IF NOT EXISTS vector",
            f"CREATE SCHEMA IF NOT EXISTS {sch_id}",
            (
                f"CREATE TABLE {sch_id}.{tbl_id} ("
                "id TEXT PRIMARY KEY, "
                "content TEXT NOT NULL, "
                "metadata JSONB DEFAULT '{}'::jsonb, "
                f"embedding vector({int(vector_dimension)})"
                ")"
            ),
        )
        return ProvisioningPreview(
            object_family=ObjectFamily.POSTGRES_INTERNAL_OBJECT,
            destination_fqn=f"{inst_id}.{db_id}.{sch_id}.{tbl_id}",
            hierarchy_path=(inst_id, db_id, sch_id, tbl_id),
            owner_role=pg_role,
            required_privileges=(f"PG_ROLE:{pg_role}:CREATE",),
            ddl_statements=statements,
            billable_impact="Uses existing Snowflake Postgres instance storage and compute.",
            requires_confirmation=True,
            warnings=(f"Connected over TLS sslmode={sslmode} with connect_timeout={connect_timeout}s.",),
            subsystem=SubsystemBoundary.SNOWFLAKE_POSTGRES,
        )

    def preview_postgres_internal_relational_table(
        self,
        *,
        user_principal: str,
        pg_role: str,
        instance_name: str,
        pg_database: str,
        pg_schema: str,
        pg_table: str,
        include_jsonb_payload: bool = True,
        sslmode: str = "verify-ca",
        connect_timeout: int = 10,
    ) -> ProvisioningPreview:
        """Preview non-pgvector relational / JSONB memory table inside Snowflake Postgres (:5432) (HUB-W2-05)."""
        self.authz_service.authorize_postgres_operation(
            user_principal=user_principal,
            pg_role=pg_role,
            required_pg_privilege="CREATE",
            sslmode=sslmode,
            connect_timeout=connect_timeout,
            target_ref=f"{instance_name}/{pg_database}.{pg_schema}.{pg_table}",
        )
        inst_id = self.validate_identifier(instance_name, "instance_name")
        db_id = self.validate_pg_identifier(pg_database, "pg_database")
        sch_id = self.validate_pg_identifier(pg_schema, "pg_schema")
        tbl_id = self.validate_pg_identifier(pg_table, "pg_table")
        payload_col = ", payload JSONB DEFAULT '{}'::jsonb" if include_jsonb_payload else ""
        statements = (
            f"CREATE SCHEMA IF NOT EXISTS {sch_id}",
            (
                f"CREATE TABLE {sch_id}.{tbl_id} ("
                "id TEXT PRIMARY KEY, "
                "session_id TEXT, "
                "user_id TEXT NOT NULL, "
                "effective_role TEXT NOT NULL, "
                "content TEXT NOT NULL"
                f"{payload_col}, "
                "metadata JSONB DEFAULT '{}'::jsonb, "
                "created_at TIMESTAMPTZ DEFAULT NOW()"
                ")"
            ),
        )
        return ProvisioningPreview(
            object_family=ObjectFamily.POSTGRES_INTERNAL_OBJECT,
            destination_fqn=f"{inst_id}.{db_id}.{sch_id}.{tbl_id}",
            hierarchy_path=(inst_id, db_id, sch_id, tbl_id),
            owner_role=pg_role,
            required_privileges=(f"PG_ROLE:{pg_role}:CREATE",),
            ddl_statements=statements,
            billable_impact="Uses existing Snowflake Postgres instance storage and compute (non-pgvector relational/JSONB).",
            requires_confirmation=True,
            warnings=(f"Connected over TLS sslmode={sslmode} with connect_timeout={connect_timeout}s.",),
            subsystem=SubsystemBoundary.SNOWFLAKE_POSTGRES,
        )

    # ------------------------------------------------------------------
    # 2. Database & Schema
    # ------------------------------------------------------------------
    def preview_database(
        self,
        *,
        user_principal: str,
        database: str,
        comment: str | None = None,
    ) -> ProvisioningPreview:
        """Preview `CREATE DATABASE <db>` with active role ownership."""
        ctx = self.authz_service.check_operation_permission(
            user_principal=user_principal,
            operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
            target_ref=f"ACCOUNT.DATABASE.{database}",
            required_privilege="CREATE DATABASE ON ACCOUNT",
            disclosable=True,
        )
        db_id = self.validate_identifier(database, "database")
        ddl = f"CREATE DATABASE {db_id}"
        if comment:
            safe_comment = comment.replace("'", "''")[:256]
            ddl += f" COMMENT = '{safe_comment}'"

        self.authz_service.precheck_sql_privileges(
            user_principal=user_principal,
            sql_statement=ddl,
            operation="CREATE_DATABASE",
            target_ref=db_id,
            disclosable=True,
        )
        return ProvisioningPreview(
            object_family=ObjectFamily.DATABASE,
            destination_fqn=db_id,
            hierarchy_path=("ACCOUNT", db_id),
            owner_role=ctx.active_role,
            required_privileges=("CREATE DATABASE ON ACCOUNT",),
            ddl_statements=(ddl,),
            billable_impact="Metadata + storage consumption for objects created inside the database.",
        )

    def preview_schema(
        self,
        *,
        user_principal: str,
        database: str,
        schema: str,
    ) -> ProvisioningPreview:
        """Preview `CREATE SCHEMA <db>.<schema>` respecting `Account -> Database -> Schema`."""
        db_id = self.validate_identifier(database, "database")
        sch_id = self.validate_identifier(schema, "schema")
        fqn = f"{db_id}.{sch_id}"
        ctx = self.authz_service.check_operation_permission(
            user_principal=user_principal,
            operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
            target_ref=fqn,
            required_privilege="CREATE SCHEMA ON DATABASE",
            disclosable=True,
        )
        ddl = f"CREATE SCHEMA {fqn}"
        self.authz_service.precheck_sql_privileges(
            user_principal=user_principal,
            sql_statement=ddl,
            operation="CREATE_SCHEMA",
            target_ref=fqn,
            disclosable=True,
        )
        return ProvisioningPreview(
            object_family=ObjectFamily.SCHEMA,
            destination_fqn=fqn,
            hierarchy_path=("ACCOUNT", db_id, sch_id),
            owner_role=ctx.active_role,
            required_privileges=("USAGE ON DATABASE", "CREATE SCHEMA ON DATABASE"),
            ddl_statements=(ddl,),
            billable_impact="Metadata namespace within parent database.",
        )

    # ------------------------------------------------------------------
    # 3. Snowflake Tables (Standard, Hybrid, Iceberg, Interactive, Dynamic)
    # ------------------------------------------------------------------
    def preview_table(
        self,
        *,
        user_principal: str,
        table_family: ObjectFamily,
        database: str,
        schema: str,
        table: str,
        columns: list[tuple[str, str]] | None = None,
        primary_key_columns: list[str] | None = None,
        secondary_index_columns: list[str] | None = None,
        expected_embedding_dim: int | None = None,
        external_volume: str | None = None,
        base_location: str | None = None,
        target_lag: str | None = None,
        warehouse: str | None = None,
        as_select_sql: str | None = None,
        refresh_mode: str = "AUTO",
    ) -> ProvisioningPreview:
        """Preview creation of Standard, Hybrid, Iceberg, Interactive, or Dynamic tables."""
        db_id = self.validate_identifier(database, "database")
        sch_id = self.validate_identifier(schema, "schema")
        tbl_id = self.validate_identifier(table, "table")
        fqn = f"{db_id}.{sch_id}.{tbl_id}"

        ctx = self.authz_service.check_operation_permission(
            user_principal=user_principal,
            operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
            target_ref=fqn,
            disclosable=True,
        )

        vector_columns: set[str] = set()
        col_defs: list[str] = []
        for col_name, col_type in columns or []:
            c_id = self.validate_identifier(col_name, "column")
            c_type = col_type.strip().upper()
            self._ensure_no_or_replace(c_type, ctx.active_role, f"CREATE_{table_family.value}")
            vec_dim = self._validate_vector_column_spec(
                c_type,
                active_role=ctx.active_role,
                operation=f"CREATE_{table_family.value}",
                expected_embedding_dim=expected_embedding_dim,
            )
            if vec_dim is not None:
                vector_columns.add(c_id)
            col_defs.append(f"{c_id} {c_type}")

        if table_family == ObjectFamily.STANDARD_TABLE:
            if not col_defs:
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_STANDARD_TABLE",
                    reason="At least one column definition is required for STANDARD_TABLE.",
                )
            ddl = f"CREATE TABLE {fqn} ({', '.join(col_defs)})"
            privs = ("USAGE ON DATABASE", "USAGE ON SCHEMA", "CREATE TABLE ON SCHEMA")
            impact = "Standard Snowflake micro-partition storage."

        elif table_family == ObjectFamily.HYBRID_TABLE:
            if not col_defs:
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_HYBRID_TABLE",
                    reason="Hybrid tables require column definitions and a PRIMARY KEY.",
                )
            if not primary_key_columns:
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_HYBRID_TABLE",
                    reason="Hybrid tables require an explicit PRIMARY KEY constraint.",
                )
            pk_ids = [self.validate_identifier(pk, "primary_key") for pk in primary_key_columns]
            idx_ids = [
                self.validate_identifier(ix, "secondary_index")
                for ix in (secondary_index_columns or [])
            ]
            # Enforce verified Snowflake constraint: VECTOR cannot be PK or secondary index key in Hybrid Table
            forbidden_vec_keys = (set(pk_ids) | set(idx_ids)) & vector_columns
            if forbidden_vec_keys:
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_HYBRID_TABLE",
                    reason=(
                        f"In a Hybrid Table, VECTOR column(s) {sorted(forbidden_vec_keys)} "
                        "cannot be part of PRIMARY KEY or secondary INDEX keys."
                    ),
                )
            pk_clause = f"PRIMARY KEY ({', '.join(pk_ids)})"
            ddl = f"CREATE HYBRID TABLE {fqn} ({', '.join([*col_defs, pk_clause])})"
            privs = ("USAGE ON DATABASE", "USAGE ON SCHEMA", "CREATE HYBRID TABLE ON SCHEMA")
            impact = "Unistore row-store + operational index storage and compute."

        elif table_family == ObjectFamily.ICEBERG_TABLE:
            if not col_defs or not external_volume:
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_ICEBERG_TABLE",
                    reason="Iceberg tables require columns and an EXTERNAL_VOLUME.",
                )
            if vector_columns:
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_ICEBERG_TABLE",
                    reason="Native VECTOR column type is not supported on Iceberg tables without qualification.",
                )
            vol_id = self.validate_identifier(external_volume, "external_volume")
            loc = (base_location or tbl_id.lower()).replace("'", "")
            ddl = (
                f"CREATE ICEBERG TABLE {fqn} ({', '.join(col_defs)}) "
                f"EXTERNAL_VOLUME = '{vol_id}' "
                f"CATALOG = 'SNOWFLAKE' "
                f"BASE_LOCATION = '{loc}'"
            )
            privs = (
                "USAGE ON DATABASE",
                "USAGE ON SCHEMA",
                "USAGE ON EXTERNAL VOLUME",
                "CREATE ICEBERG TABLE ON SCHEMA",
            )
            impact = f"Snowflake-managed Iceberg metadata + storage on EXTERNAL_VOLUME '{vol_id}'."

        elif table_family == ObjectFamily.INTERACTIVE_TABLE:
            if not target_lag or not self._TARGET_LAG_RE.match(target_lag.strip()):
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_INTERACTIVE_TABLE",
                    reason="Interactive tables require a valid TARGET_LAG (e.g., '1 minute').",
                )
            if not warehouse or not as_select_sql or not as_select_sql.strip().upper().startswith("SELECT "):
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_INTERACTIVE_TABLE",
                    reason="Interactive tables require a WAREHOUSE and a read-only AS SELECT query.",
                )
            wh_id = self.validate_identifier(warehouse, "warehouse")
            self._ensure_no_or_replace(as_select_sql, ctx.active_role, "CREATE_INTERACTIVE_TABLE")
            ddl = (
                f"CREATE INTERACTIVE TABLE {fqn} "
                f"TARGET_LAG = '{target_lag.strip()}' "
                f"WAREHOUSE = {wh_id} "
                f"AS {as_select_sql.strip()}"
            )
            privs = (
                "USAGE ON DATABASE",
                "USAGE ON SCHEMA",
                "USAGE ON WAREHOUSE",
                "CREATE INTERACTIVE TABLE ON SCHEMA",
                "SELECT ON SOURCE TABLE",
            )
            impact = f"Low-latency interactive table refreshed with TARGET_LAG='{target_lag.strip()}' on warehouse {wh_id}."

        elif table_family == ObjectFamily.DYNAMIC_TABLE:
            if not target_lag or not self._TARGET_LAG_RE.match(target_lag.strip()):
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_DYNAMIC_TABLE",
                    reason="Dynamic tables require a valid TARGET_LAG (e.g., '5 minutes' or 'DOWNSTREAM').",
                )
            if not warehouse or not as_select_sql or not as_select_sql.strip().upper().startswith("SELECT "):
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_DYNAMIC_TABLE",
                    reason="Dynamic tables require a WAREHOUSE and a read-only AS SELECT query.",
                )
            mode_clean = refresh_mode.strip().upper()
            if mode_clean not in {"AUTO", "INCREMENTAL", "FULL"}:
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_DYNAMIC_TABLE",
                    reason=f"Unsupported Dynamic Table REFRESH_MODE '{refresh_mode}'.",
                )
            wh_id = self.validate_identifier(warehouse, "warehouse")
            self._ensure_no_or_replace(as_select_sql, ctx.active_role, "CREATE_DYNAMIC_TABLE")
            ddl = (
                f"CREATE DYNAMIC TABLE {fqn} "
                f"TARGET_LAG = '{target_lag.strip()}' "
                f"WAREHOUSE = {wh_id} "
                f"REFRESH_MODE = {mode_clean} "
                f"AS {as_select_sql.strip()}"
            )
            privs = (
                "USAGE ON DATABASE",
                "USAGE ON SCHEMA",
                "USAGE ON WAREHOUSE",
                "CREATE DYNAMIC TABLE ON SCHEMA",
                "SELECT ON SOURCE TABLE",
            )
            impact = f"Scheduled Dynamic Table refresh (TARGET_LAG='{target_lag.strip()}', WAREHOUSE={wh_id})."

        else:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation="PREVIEW_TABLE",
                reason=f"Unsupported table family '{table_family}'.",
            )

        self.authz_service.precheck_sql_privileges(
            user_principal=user_principal,
            sql_statement=ddl,
            operation=f"CREATE_{table_family.value}",
            target_ref=fqn,
            disclosable=True,
        )
        return ProvisioningPreview(
            object_family=table_family,
            destination_fqn=fqn,
            hierarchy_path=("ACCOUNT", db_id, sch_id, tbl_id),
            owner_role=ctx.active_role,
            required_privileges=privs,
            ddl_statements=(ddl,),
            billable_impact=impact,
        )

    # ------------------------------------------------------------------
    # 4. Snowflake Stages (Internal Named Stage & External Stage)
    # ------------------------------------------------------------------
    def preview_stage(
        self,
        *,
        user_principal: str,
        database: str,
        schema: str,
        stage: str,
        is_external: bool = False,
        external_url: str | None = None,
        storage_integration: str | None = None,
        enable_directory: bool = True,
        auto_refresh: bool = False,
    ) -> ProvisioningPreview:
        """Preview governed creation of an Internal Named Stage or External Stage."""
        db_id = self.validate_identifier(database, "database")
        sch_id = self.validate_identifier(schema, "schema")
        stg_id = self.validate_identifier(stage, "stage")
        fqn = f"{db_id}.{sch_id}.{stg_id}"

        ctx = self.authz_service.check_operation_permission(
            user_principal=user_principal,
            operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
            target_ref=fqn,
            disclosable=True,
        )

        if not is_external:
            dir_clause = " DIRECTORY = (ENABLE = TRUE)" if enable_directory else ""
            ddl = f"CREATE STAGE {fqn}{dir_clause} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
            privs = ("USAGE ON DATABASE", "USAGE ON SCHEMA", "CREATE STAGE ON SCHEMA")
            family = ObjectFamily.INTERNAL_STAGE
            impact = f"Internal Named Stage in {db_id}.{sch_id} with server-side encryption."
        else:
            if not external_url or not storage_integration:
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_EXTERNAL_STAGE",
                    reason="External stages require both URL and an authorized STORAGE_INTEGRATION.",
                )
            clean_url = external_url.strip()
            if not any(clean_url.lower().startswith(scheme) for scheme in self._ALLOWED_EXTERNAL_STAGE_SCHEMES):
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_EXTERNAL_STAGE",
                    reason=f"External stage URL must start with one of {self._ALLOWED_EXTERNAL_STAGE_SCHEMES}.",
                )
            if any(tok in clean_url.lower() for tok in ("aws_secret", "sas=", "token=", "password=", "key=")):
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation="CREATE_EXTERNAL_STAGE",
                    reason="Inline cloud credentials in external stage URL are forbidden; use STORAGE_INTEGRATION.",
                )
            int_id = self.validate_identifier(storage_integration, "storage_integration")
            if enable_directory:
                dir_clause = " DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)" if auto_refresh else " DIRECTORY = (ENABLE = TRUE)"
            else:
                dir_clause = ""
            ddl = f"CREATE STAGE {fqn} URL = '{clean_url}' STORAGE_INTEGRATION = {int_id}{dir_clause}"
            privs = (
                "USAGE ON DATABASE",
                "USAGE ON SCHEMA",
                "CREATE STAGE ON SCHEMA",
                "USAGE ON INTEGRATION",
            )
            family = ObjectFamily.EXTERNAL_STAGE
            impact = f"External Stage bound to STORAGE_INTEGRATION {int_id} (PUT not supported; external write policy applies)."

        self.authz_service.precheck_sql_privileges(
            user_principal=user_principal,
            sql_statement=ddl,
            operation=f"CREATE_{family.value}",
            target_ref=fqn,
            disclosable=True,
        )
        return ProvisioningPreview(
            object_family=family,
            destination_fqn=fqn,
            hierarchy_path=("ACCOUNT", db_id, sch_id, stg_id),
            owner_role=ctx.active_role,
            required_privileges=privs,
            ddl_statements=(ddl,),
            billable_impact=impact,
        )

    # ------------------------------------------------------------------
    # 5. Hierarchical Privilege Prechecks & HUB-W2-04 Exclusion
    # ------------------------------------------------------------------
    @staticmethod
    def enforce_hub_object_exclusion(
        object_kind: str,
        *,
        source: str = "hub",
        active_role: str = "PUBLIC",
    ) -> None:
        """Reject FUNCTION / PROCEDURE creation from Snowflake Langflow Hub (`HUB-W2-04`)."""
        kind_clean = (object_kind or "").strip().lower()
        src_clean = (source or "hub").strip().lower()
        if src_clean == "hub" and kind_clean in {
            "function",
            "procedure",
            "udf",
            "udtf",
            "stored_procedure",
            "stored procedure",
        }:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=active_role,
                operation="HUB_PROVISION_OBJECT",
                reason=(
                    f"Creation of '{object_kind}' is strictly excluded from Snowflake Langflow Hub (HUB-W2-04). "
                    "Use 'LLM Operations -> Stored Procedure / Function' in the sidebar instead."
                ),
            )

    def can_create_database(self, *, user_principal: str) -> dict[str, Any]:
        """Check whether active role can execute CREATE DATABASE ON ACCOUNT."""
        ctx = self.authz_service.get_session_context(user_principal)
        try:
            self.authz_service.check_operation_permission(
                user_principal=user_principal,
                operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
                target_ref="ACCOUNT.DATABASE._PRECHECK",
                required_privilege="CREATE DATABASE ON ACCOUNT",
                disclosable=True,
            )
            return {"allowed": True, "active_role": ctx.active_role, "warning": None}
        except SnowflakeGovernanceError as exc:
            return {
                "allowed": False,
                "active_role": ctx.active_role,
                "warning": f"Il ruolo attivo ({ctx.active_role}) non dispone dei privilegi per creare un Database sull'Account.",
                "error_code": exc.error_code.value,
            }

    def can_create_schema(self, *, user_principal: str, database: str) -> dict[str, Any]:
        """Check whether active role can execute CREATE SCHEMA in `<database>`."""
        ctx = self.authz_service.get_session_context(user_principal)
        db_id = self.validate_identifier(database, "database")
        try:
            self.authz_service.check_operation_permission(
                user_principal=user_principal,
                operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
                target_ref=f"{db_id}._PRECHECK",
                required_privilege="CREATE SCHEMA ON DATABASE",
                disclosable=True,
            )
            return {"allowed": True, "active_role": ctx.active_role, "database": db_id, "warning": None}
        except SnowflakeGovernanceError as exc:
            return {
                "allowed": False,
                "active_role": ctx.active_role,
                "database": db_id,
                "warning": f"Il ruolo attivo ({ctx.active_role}) non dispone dei privilegi per creare uno Schema in {db_id}.",
                "error_code": exc.error_code.value,
            }

    def can_create_object(
        self,
        *,
        user_principal: str,
        database: str,
        schema: str,
        object_kind: str = "table",
    ) -> dict[str, Any]:
        """Check whether active role can create an object inside `<database>.<schema>`."""
        ctx = self.authz_service.get_session_context(user_principal)
        db_id = self.validate_identifier(database, "database")
        sch_id = self.validate_identifier(schema, "schema")
        fqn = f"{db_id}.{sch_id}"
        try:
            self.authz_service.check_operation_permission(
                user_principal=user_principal,
                operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
                target_ref=f"{fqn}._PRECHECK_{object_kind.upper()}",
                required_privilege=f"CREATE {object_kind.upper()} ON SCHEMA",
                disclosable=True,
            )
            return {"allowed": True, "active_role": ctx.active_role, "schema_fqn": fqn, "warning": None}
        except SnowflakeGovernanceError as exc:
            return {
                "allowed": False,
                "active_role": ctx.active_role,
                "schema_fqn": fqn,
                "warning": f"Il ruolo attivo ({ctx.active_role}) non dispone dei privilegi per creare questo oggetto in {fqn}.",
                "error_code": exc.error_code.value,
            }

    def evaluate_hierarchy_privileges(
        self,
        *,
        user_principal: str,
        database: str,
        schema: str,
    ) -> dict[str, Any]:
        """Return granular per-hierarchy creation privilege flags for Hub Section 1 & Object Provisioner (`HUB-W2-03`)."""
        db_check = self.can_create_database(user_principal=user_principal)
        sch_check = self.can_create_schema(user_principal=user_principal, database=database)
        obj_check = self.can_create_object(user_principal=user_principal, database=database, schema=schema, object_kind="table")
        stg_check = self.can_create_object(user_principal=user_principal, database=database, schema=schema, object_kind="stage")
        return {
            "active_role": db_check["active_role"],
            "database": database.upper(),
            "schema": schema.upper(),
            "can_create_database": db_check["allowed"],
            "can_create_schema": sch_check["allowed"],
            "can_create_object": obj_check["allowed"],
            "can_create_table": obj_check["allowed"],
            "can_create_stage": stg_check["allowed"],
            "can_create_view": obj_check["allowed"],
            "can_create_postgres": db_check["allowed"],
            "warning_message": obj_check["warning"] or sch_check["warning"] or db_check["warning"],
        }

    def build_routine_ddl(
        self,
        *,
        routine_kind: str,
        database: str,
        schema: str,
        routine_name: str,
        signature_args: str = "",
        return_type: str = "VARIANT",
        language: str = "SQL",
        body: str = "SELECT CURRENT_ROLE()",
        execute_as: str = "CALLER",
        runtime_version: str = "3.12",
        packages: tuple[str, ...] = ("snowflake-snowpark-python",),
        handler: str = "run",
        stage_imports: tuple[str, ...] = (),
        child_procedures: tuple[str, ...] = (),
        source: str = "llm_operations",
        active_role: str = "ACCOUNTADMIN",
    ) -> str:
        """Build governed DDL for Stored Procedure or UDF/UDTF in `LLM Operations` (`LLM-W2-01`)."""
        self.enforce_hub_object_exclusion(routine_kind, source=source, active_role=active_role)
        db_id = self.validate_identifier(database, "database")
        sch_id = self.validate_identifier(schema, "schema")
        rt_id = self.validate_identifier(routine_name, "routine_name")
        fqn = f"{db_id}.{sch_id}.{rt_id}"
        lang = (language or "SQL").strip().upper()
        kind_upper = (routine_kind or "PROCEDURE").strip().upper()
        is_proc = "PROC" in kind_upper
        kw = "PROCEDURE" if is_proc else "FUNCTION"
        exec_clause = f"\nEXECUTE AS {execute_as.strip().upper().replace('EXECUTE AS ', '')}" if is_proc else ""
        imports_clause = (
            f"\nIMPORTS = ({', '.join(repr(imp) for imp in stage_imports)})" if stage_imports else ""
        )

        child_calls_sql = ""
        if is_proc and child_procedures:
            calls = [f"CALL {child.strip()}();" for child in child_procedures if child.strip()]
            if calls:
                child_calls_sql = "\n    -- Graphical Child Procedure Chain\n    " + "\n    ".join(calls) + "\n"

        if lang == "PYTHON":
            pkgs = ", ".join(repr(p) for p in (packages or ("snowflake-snowpark-python",)))
            return (
                f"CREATE OR REPLACE {kw} {fqn}({signature_args})\n"
                f"RETURNS {return_type}\n"
                f"LANGUAGE PYTHON\n"
                f"RUNTIME_VERSION = '{runtime_version}'\n"
                f"PACKAGES = ({pkgs})"
                f"{imports_clause}\n"
                f"HANDLER = '{handler}'"
                f"{exec_clause}\n"
                f"AS\n$$\n{body}\n$$;"
            )
        if is_proc:
            return (
                f"CREATE OR REPLACE PROCEDURE {fqn}({signature_args})\n"
                f"RETURNS {return_type}\n"
                f"LANGUAGE SQL"
                f"{exec_clause}\n"
                f"AS\n$$\nBEGIN{child_calls_sql}\n    {body}\nEND;\n$$;"
            )
        return (
            f"CREATE OR REPLACE FUNCTION {fqn}({signature_args})\n"
            f"RETURNS {return_type}\n"
            f"LANGUAGE SQL\n"
            f"AS\n$$\n    {body}\n$$;"
        )

    # ------------------------------------------------------------------
    # Confirmed Execution with Epoch Verification & Synchronized Audit
    # ------------------------------------------------------------------
    def execute_previewed_provisioning(
        self,
        *,
        user_principal: str,
        preview: ProvisioningPreview,
        user_confirmed: bool,
        expected_epoch: int | None = None,
    ) -> ProvisioningExecutionResult:
        """Execute a previewed provisioning plan only after explicit user confirmation."""
        ctx = self.authz_service.get_session_context(user_principal)
        if expected_epoch is not None:
            self.authz_service.validate_epoch(user_principal, expected_epoch)

        if not user_confirmed:
            self.authz_service._record_audit(
                authenticated_user=user_principal,
                effective_role=ctx.active_role,
                identity_type=ctx.identity_type,
                context_epoch=ctx.context_epoch,
                operation=f"EXECUTE_{preview.object_family.value}",
                target_ref=preview.destination_fqn,
                status="DENIED_UNCONFIRMED",
                error_code=SnowflakeErrorCode.UNSUPPORTED_CAPABILITY.value,
                subsystem=preview.subsystem.value,
                project_id=ctx.project_id,
            )
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation=f"EXECUTE_{preview.object_family.value}",
                reason="Explicit user confirmation (user_confirmed=True) is mandatory before creating objects.",
                subsystem=preview.subsystem,
            )

        query_ids: list[str] = []
        if preview.subsystem == SubsystemBoundary.SNOWFLAKE_POSTGRES:
            self.authz_service.authorize_postgres_operation(
                user_principal=user_principal,
                pg_role=preview.owner_role,
                required_pg_privilege="CREATE",
                target_ref=preview.destination_fqn,
            )
            for stmt in preview.ddl_statements:
                self._ensure_no_or_replace(stmt, ctx.active_role, f"EXECUTE_{preview.object_family.value}")
                qid = self._pg_ddl_executor(preview.owner_role, stmt)
                query_ids.append(qid)
        else:
            # Re-verify permission and zero-credit EXPLAIN_PRIVILEGES at execution time
            self.authz_service.check_operation_permission(
                user_principal=user_principal,
                operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
                target_ref=preview.destination_fqn,
                disclosable=True,
            )
            for stmt in preview.ddl_statements:
                self._ensure_no_or_replace(stmt, ctx.active_role, f"EXECUTE_{preview.object_family.value}")
                self.authz_service.precheck_sql_privileges(
                    user_principal=user_principal,
                    sql_statement=stmt,
                    operation=f"EXECUTE_{preview.object_family.value}",
                    target_ref=preview.destination_fqn,
                    disclosable=True,
                )
                qid = self._ddl_executor(ctx.active_role, stmt)
                query_ids.append(qid)

        audit_event = self.authz_service._record_audit(
            authenticated_user=user_principal,
            effective_role=ctx.active_role,
            identity_type=ctx.identity_type,
            context_epoch=ctx.context_epoch,
            operation=f"CREATE_{preview.object_family.value}",
            target_ref=preview.destination_fqn,
            status="CREATED",
            subsystem=preview.subsystem.value,
            project_id=ctx.project_id,
            metadata={
                "owner_role": preview.owner_role,
                "hierarchy_path": list(preview.hierarchy_path),
                "query_ids": query_ids,
            },
        )
        return ProvisioningExecutionResult(
            object_family=preview.object_family,
            destination_fqn=preview.destination_fqn,
            owner_role=preview.owner_role,
            executed_statements=preview.ddl_statements,
            audit_event_id=audit_event.event_id,
            status="CREATED",
            query_ids=tuple(query_ids),
        )
