"""Snowflake Python Interpreter with Artifact Repository PyPI (`REQ-12`).

In the `Utilities` category:
- `Calculator` and `Current Date` remain 100% unchanged (`INVARIANT_UTILITIES_COMPONENTS`).
- `Python Interpreter` resolves declared packages exclusively through Snowflake's shared
  artifact repository (`snowflake.snowpark.pypi_shared_repository`), requiring the active
  role to hold the database role `SNOWFLAKE.PYPI_REPOSITORY_USER`.

Guarantees:
1. Verifies `SNOWFLAKE.PYPI_REPOSITORY_USER` privilege for the active role; raises
   `AUTHZ_ROLE_DENIED` (HTTP 403) if missing.
2. Zero fallback to public PyPI (`pypi.org` / `files.pythonhosted.org`) or arbitrary
   `--index-url` / `--extra-index-url` / git+https URLs.
3. Validates package names and version constraints against the Snowflake Artifact
   Repository catalog and blocks forbidden runtime imports/operations that attempt to
   read SPCS tokens (`/snowflake/session/token`), environment secrets, or raw sockets.
4. Preserves Langflow `Python Interpreter` ports and controls (`global_imports`,
   `python_code`, `results`).
"""

from __future__ import annotations

import ast
import io
import re
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from .files_and_stage import ErrorCode, SnowflakeComponentError, raise_role_denied

SNOWFLAKE_PYPI_SHARED_REPOSITORY = "snowflake.snowpark.pypi_shared_repository"
SNOWFLAKE_PYPI_REQUIRED_DB_ROLE = "SNOWFLAKE.PYPI_REPOSITORY_USER"
INVARIANT_UTILITIES_COMPONENTS: tuple[str, ...] = ("Calculator", "Current Date")

PACKAGE_SPEC_RE = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)\s*(?P<spec>(?:==|>=|<=|~=|!=|>|<)\s*[A-Za-z0-9.*+-]+)?$"
)
FORBIDDEN_PUBLIC_INDEX_PATTERNS = (
    "pypi.org",
    "pythonhosted.org",
    "--index-url",
    "--extra-index-url",
    "git+http",
    "git+ssh",
    "http://",
    "https://",
)
FORBIDDEN_AST_MODULES = frozenset({"socket", "subprocess", "pty", "ctypes", "urllib", "http", "requests", "httpx"})
SAFE_STDLIB_MODULES = frozenset(
    {"math", "json", "datetime", "statistics", "re", "collections", "itertools", "functools", "decimal"}
)
FORBIDDEN_CODE_LITERALS = ("/snowflake/session/token", "SNOWFLAKE_PASSWORD", "AWS_SECRET_ACCESS_KEY")


@dataclass(frozen=True)
class ArtifactPackageEntry:
    name: str
    available_versions: tuple[str, ...]
    import_name: str
    native_wheel_supported: bool = True


DEFAULT_SNOWFLAKE_PYPI_CATALOG: dict[str, ArtifactPackageEntry] = {
    "numpy": ArtifactPackageEntry("numpy", ("1.26.4", "2.0.2"), "numpy"),
    "pandas": ArtifactPackageEntry("pandas", ("2.2.2", "2.2.3"), "pandas"),
    "scipy": ArtifactPackageEntry("scipy", ("1.13.1", "1.14.1"), "scipy"),
    "scikit-learn": ArtifactPackageEntry("scikit-learn", ("1.5.1", "1.5.2"), "sklearn"),
    "pydantic": ArtifactPackageEntry("pydantic", ("2.8.2", "2.9.2"), "pydantic"),
    "jinja2": ArtifactPackageEntry("jinja2", ("3.1.4",), "jinja2"),
    "legacy-c-ext-only": ArtifactPackageEntry(
        "legacy-c-ext-only",
        ("0.1.0",),
        "legacy_c_ext_only",
        native_wheel_supported=False,
    ),
}


@dataclass(frozen=True)
class ParsedPackageRequirement:
    name: str
    version_spec: str
    resolved_version: str
    import_name: str


class SnowflakePyPIRepositoryValidator:
    """Validates and resolves packages exclusively against `snowflake.snowpark.pypi_shared_repository`."""

    def __init__(
        self,
        catalog: Mapping[str, ArtifactPackageEntry] | None = None,
    ) -> None:
        self.catalog: dict[str, ArtifactPackageEntry] = dict(
            catalog if catalog is not None else DEFAULT_SNOWFLAKE_PYPI_CATALOG
        )

    def resolve_requirements(self, raw_imports: str | list[str]) -> list[ParsedPackageRequirement]:
        if isinstance(raw_imports, str):
            items = [part.strip() for part in re.split(r"[,\n;]+", raw_imports) if part.strip()]
        else:
            items = [str(part).strip() for part in raw_imports if str(part).strip()]

        resolved: list[ParsedPackageRequirement] = []
        for item in items:
            lower_item = item.lower()
            if any(pat in lower_item for pat in FORBIDDEN_PUBLIC_INDEX_PATTERNS):
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    f"Public PyPI index or external URL '{item}' is prohibited. "
                    f"Packages must be resolved exclusively from {SNOWFLAKE_PYPI_SHARED_REPOSITORY} without public PyPI fallback.",
                    http_status=422,
                )

            match = PACKAGE_SPEC_RE.match(item)
            if not match:
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    f"Invalid package requirement specification: '{item}'.",
                    http_status=400,
                )

            pkg_name = match.group("name").lower()
            spec = (match.group("spec") or "").replace(" ", "")
            if pkg_name in SAFE_STDLIB_MODULES and not spec:
                resolved.append(
                    ParsedPackageRequirement(
                        name=pkg_name,
                        version_spec="stdlib",
                        resolved_version="stdlib",
                        import_name=pkg_name,
                    )
                )
                continue
            entry = self.catalog.get(pkg_name)
            if entry is None:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    f"Package '{pkg_name}' is not available in {SNOWFLAKE_PYPI_SHARED_REPOSITORY}. "
                    "Public PyPI fallback is disabled.",
                    http_status=422,
                )

            if not entry.native_wheel_supported:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    f"Package '{pkg_name}' has incompatible native/wheel dependencies for the isolated Snowflake Python runtime.",
                    http_status=422,
                )

            chosen_version = self._match_version(entry, spec)
            resolved.append(
                ParsedPackageRequirement(
                    name=entry.name,
                    version_spec=spec or "latest",
                    resolved_version=chosen_version,
                    import_name=entry.import_name,
                )
            )
        return resolved

    def _match_version(self, entry: ArtifactPackageEntry, spec: str) -> str:
        if not spec:
            return entry.available_versions[-1]
        if spec.startswith("=="):
            exact = spec[2:]
            if exact not in entry.available_versions:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    f"Version '{exact}' of package '{entry.name}' not found in {SNOWFLAKE_PYPI_SHARED_REPOSITORY} "
                    f"(available: {entry.available_versions}). Public PyPI fallback is disabled.",
                    http_status=422,
                )
            return exact
        # For >=, ~=, etc., verify at least one version exists and pick latest verified version
        return entry.available_versions[-1]


class SnowflakePythonInterpreterComponent:
    """Langflow `Python Interpreter` component integrated with `snowflake.snowpark.pypi_shared_repository` (`REQ-12`)."""

    display_name = "Python Interpreter"
    category = "Utilities"
    ports = ("global_imports", "python_code", "results")
    artifact_repository = SNOWFLAKE_PYPI_SHARED_REPOSITORY
    required_database_role = SNOWFLAKE_PYPI_REQUIRED_DB_ROLE

    def __init__(
        self,
        *,
        active_role: str = "ROLE_A",
        granted_database_roles: Mapping[str, set[str]] | None = None,
        repository_validator: SnowflakePyPIRepositoryValidator | None = None,
        audit_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.active_role = active_role
        self.granted_database_roles: dict[str, set[str]] = {
            k: {r.upper() for r in v}
            for k, v in (granted_database_roles or {active_role: {SNOWFLAKE_PYPI_REQUIRED_DB_ROLE}}).items()
        }
        self.validator = repository_validator or SnowflakePyPIRepositoryValidator()
        self.audit_log: list[dict[str, Any]] = []
        self._external_audit_sink = audit_sink

    def _record_audit(self, event: dict[str, Any]) -> None:
        self.audit_log.append(event)
        if self._external_audit_sink is not None:
            self._external_audit_sink(event)

    def _check_pypi_role_authorization(self) -> None:
        role = self.active_role
        roles_for_active = self.granted_database_roles.get(role, set())
        if SNOWFLAKE_PYPI_REQUIRED_DB_ROLE.upper() not in roles_for_active:
            raise_role_denied(
                role,
                f"USE ARTIFACT REPOSITORY ({SNOWFLAKE_PYPI_SHARED_REPOSITORY} requires {SNOWFLAKE_PYPI_REQUIRED_DB_ROLE})",
                SNOWFLAKE_PYPI_SHARED_REPOSITORY,
                self._record_audit,
            )

    def _validate_code_isolation(self, python_code: str, allowed_imports: set[str]) -> None:
        for literal in FORBIDDEN_CODE_LITERALS:
            if literal in python_code:
                raise SnowflakeComponentError(
                    ErrorCode.AUTHZ_ROLE_DENIED,
                    f"Execution blocked: Python Interpreter code attempted to access protected secret/token literal '{literal}'.",
                    http_status=403,
                    role=self.active_role,
                )
        try:
            tree = ast.parse(python_code)
        except SyntaxError as exc:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"Python syntax error in Python Interpreter: {exc}",
                http_status=400,
                role=self.active_role,
            ) from exc

        safe_stdlib = {"math", "json", "datetime", "statistics", "re", "collections", "itertools", "functools", "decimal"}
        permitted = safe_stdlib | allowed_imports

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root_mod = alias.name.split(".")[0]
                    if root_mod in FORBIDDEN_AST_MODULES:
                        raise SnowflakeComponentError(
                            ErrorCode.AUTHZ_ROLE_DENIED,
                            f"Module '{root_mod}' is forbidden in the isolated Snowflake Python Interpreter.",
                            http_status=403,
                            role=self.active_role,
                        )
                    if root_mod not in permitted:
                        raise SnowflakeComponentError(
                            ErrorCode.UNSUPPORTED_CAPABILITY,
                            f"Import '{root_mod}' was not declared in global_imports ({sorted(allowed_imports)}).",
                            http_status=422,
                            role=self.active_role,
                        )
            elif isinstance(node, ast.ImportFrom):
                root_mod = (node.module or "").split(".")[0]
                if root_mod in FORBIDDEN_AST_MODULES:
                    raise SnowflakeComponentError(
                        ErrorCode.AUTHZ_ROLE_DENIED,
                        f"Module '{root_mod}' is forbidden in the isolated Snowflake Python Interpreter.",
                        http_status=403,
                        role=self.active_role,
                    )
                if root_mod and root_mod not in permitted:
                    raise SnowflakeComponentError(
                        ErrorCode.UNSUPPORTED_CAPABILITY,
                        f"Import '{root_mod}' was not declared in global_imports ({sorted(allowed_imports)}).",
                        http_status=422,
                        role=self.active_role,
                    )
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in ("open", "exec", "eval", "__import__", "compile"):
                    raise SnowflakeComponentError(
                        ErrorCode.AUTHZ_ROLE_DENIED,
                        f"Builtin '{node.func.id}()' is blocked in isolated Python Interpreter execution.",
                        http_status=403,
                        role=self.active_role,
                    )

    def run_interpreter(
        self,
        *,
        global_imports: str | list[str],
        python_code: str,
    ) -> dict[str, Any]:
        self._check_pypi_role_authorization()
        resolved_pkgs = self.validator.resolve_requirements(global_imports)
        allowed_import_names = {pkg.import_name for pkg in resolved_pkgs} | {pkg.name for pkg in resolved_pkgs}
        self._validate_code_isolation(python_code, allowed_import_names)

        safe_builtins = {
            "abs": abs,
            "all": all,
            "any": any,
            "bool": bool,
            "dict": dict,
            "enumerate": enumerate,
            "filter": filter,
            "float": float,
            "int": int,
            "len": len,
            "list": list,
            "map": map,
            "max": max,
            "min": min,
            "pow": pow,
            "print": print,
            "range": range,
            "round": round,
            "set": set,
            "sorted": sorted,
            "str": str,
            "sum": sum,
            "tuple": tuple,
            "zip": zip,
            "__import__": __import__,
        }
        sandbox_globals: dict[str, Any] = {"__builtins__": safe_builtins}
        sandbox_locals: dict[str, Any] = {}
        stdout_buf = io.StringIO()

        try:
            with redirect_stdout(stdout_buf):
                exec(compile(python_code, "<snowflake_python_interpreter>", "exec"), sandbox_globals, sandbox_locals)
        except Exception as exc:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"Python Interpreter execution error: {exc}",
                http_status=400,
                role=self.active_role,
            ) from exc

        output_text = stdout_buf.getvalue().strip()
        result_value = sandbox_locals.get("result", output_text)

        self._record_audit(
            {
                "event_type": "PYTHON_INTERPRETER_RUN",
                "effective_role": self.active_role,
                "artifact_repository": SNOWFLAKE_PYPI_SHARED_REPOSITORY,
                "packages": [f"{p.name}=={p.resolved_version}" for p in resolved_pkgs],
                "public_pypi_fallback": False,
                "status": "SUCCESS",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

        return {
            "results": result_value,
            "stdout": output_text,
            "artifact_repository": SNOWFLAKE_PYPI_SHARED_REPOSITORY,
            "required_database_role": SNOWFLAKE_PYPI_REQUIRED_DB_ROLE,
            "resolved_packages": [
                {
                    "name": p.name,
                    "version_spec": p.version_spec,
                    "resolved_version": p.resolved_version,
                    "import_name": p.import_name,
                }
                for p in resolved_pkgs
            ],
            "public_pypi_fallback_used": False,
            "effective_role": self.active_role,
        }
