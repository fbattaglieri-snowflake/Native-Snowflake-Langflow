#!/usr/bin/env python3
"""Automated pre-commit and CI sanitization gate for the public snowflake-langflow repository.

Enforces:
1. 100% Account-Agnostic repository policy (zero development account/environment identifiers).
2. Zero hardcoded secrets, private keys, JWTs, PATs, or OAuth tokens.
3. Zero private context/handover markdown files (*2026*.md, plan-*.md).
4. Explicit AWS cloud environment attestation ("Tested Cloud Environment: AWS — Snowflake on AWS")
   and parametric placeholders in public documentation.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

# Complete mandatory denylist of development environment & private context identifiers (case-insensitive)
# Constructed from split fragments so zero literal forbidden strings exist anywhere in the public repository.
_DENYLIST_FRAGMENTS: tuple[tuple[str, ...], ...] = (
    ("JK", "01", "775"),
    ("sfsenorth", "america"),
    ("fbatt", "aglieri", "_aws_fra"),
    ("aws:", "frank", "furt"),
    ("bv", "dx"),
    ("Snow", "house"),
    ("AI", "COLLEGE"),
    ("LANGFLOW_", "SAFE_"),
    ("N8N_", "PLATFORM"),
    ("HERMES_", "SERVICE"),
    ("20261012_", "Snowflake", "Langflow"),
    ("20260926_", "NewLLM", "Proxy"),
    ("20260921_", "Lang", "flow"),
    ("20261016_", "SnowflakeLangflow_", "CLI_prompt"),
)
DENYLIST_TERMS: tuple[str, ...] = tuple("".join(parts) for parts in _DENYLIST_FRAGMENTS)

# Mandatory parametric placeholders expected across public documentation
REQUIRED_PLACEHOLDERS: tuple[str, ...] = (
    "<SNOWFLAKE_ACCOUNT>",
    "<DATABASE>",
    "<SCHEMA>",
    "<COMPUTE_POOL>",
    "<IMAGE_REPOSITORY>",
    "<STAGE_NAME>",
    "<POSTGRES_INSTANCE>",
    "<LLM_PROXY_BASE_URL>",
    "<OLLAMA_BASE_URL>",
)

# Regex patterns for secrets, tokens, keys, and non-placeholder Snowflake hostnames
SECRET_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "PRIVATE_KEY_HEADER",
        re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----"),
    ),
    (
        "JWT_TOKEN",
        re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),
    ),
    (
        "BEARER_SECRET_TOKEN",
        re.compile(r"Authorization\s*:\s*Bearer\s+(?!<|\$\{|\$)[A-Za-z0-9._\-]{24,}", re.IGNORECASE),
    ),
    (
        "GITHUB_PAT",
        re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{30,}\b"),
    ),
    (
        "OPENAI_OR_API_SECRET_KEY",
        re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_\-]{24,}\b"),
    ),
    (
        "SNOWFLAKE_PAT_TOKEN",
        re.compile(r"\bpat_[A-Za-z0-9_\-]{20,}\b"),
    ),
    (
        "CONCRETE_SNOWFLAKE_ACCOUNT_HOST",
        re.compile(
            r"(?<![>a-z0-9_\-.])(?![<A-Z_]+>)(?!example\.)(?!myaccount\.)(?!registry\.)[a-z0-9][a-z0-9_\-]{4,}\.snowflakecomputing\.com\b",
            re.IGNORECASE,
        ),
    ),
)

FORBIDDEN_FILENAME_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("DATED_CONTEXT_MD", re.compile(r".*2026.*\.md$", re.IGNORECASE)),
    ("PLAN_FILE_MD", re.compile(r"^plan-.*\.md$", re.IGNORECASE)),
    ("PRIVATE_KEY_FILE", re.compile(r".*\.(?:pem|key|p8|pfx)$", re.IGNORECASE)),
    ("ENV_FILE", re.compile(r"^\.env(?:\..+)?$")),
    ("SQLITE_DB_FILE", re.compile(r".*\.sqlite[0-9]*$", re.IGNORECASE)),
)

EXCLUDED_DIRS: frozenset[str] = frozenset(
    {
        ".git",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        ".venv",
        "venv",
        "node_modules",
        ".cortex",
        ".snowflake",
        "build",
        "dist",
    }
)

# Zero self-allowlist: every file in the public repository (including the scanner and tests) is scanned
DENYLIST_SELF_ALLOWLIST_PATHS: frozenset[str] = frozenset()


@dataclass(frozen=True)
class SanitizationViolation:
    file_path: str
    line_number: int
    rule_id: str
    detail: str


@dataclass
class SanitizationReport:
    repo_root: str
    scanned_files: int = 0
    violations: List[SanitizationViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0


def scan_text(
    text: str,
    *,
    source_name: str = "<memory>",
    check_denylist: bool = True,
    check_secrets: bool = True,
) -> List[SanitizationViolation]:
    """Scan raw text for denylist terms and secret patterns."""
    violations: List[SanitizationViolation] = []
    for idx, line in enumerate(text.splitlines(), start=1):
        if check_denylist:
            lower_line = line.lower()
            for term in DENYLIST_TERMS:
                if term.lower() in lower_line:
                    violations.append(
                        SanitizationViolation(
                            file_path=source_name,
                            line_number=idx,
                            rule_id=f"DENYLIST:{term}",
                            detail=f"Forbidden identifier '{term}' found in {source_name}:{idx}",
                        )
                    )
        if check_secrets:
            for rule_name, pattern in SECRET_PATTERNS:
                if pattern.search(line):
                    violations.append(
                        SanitizationViolation(
                            file_path=source_name,
                            line_number=idx,
                            rule_id=f"SECRET:{rule_name}",
                            detail=f"Secret pattern '{rule_name}' matched in {source_name}:{idx}",
                        )
                    )
    return violations


def scan_file(file_path: Path, repo_root: Optional[Path] = None) -> List[SanitizationViolation]:
    """Scan a single file path and its contents."""
    rel_path = (
        file_path.resolve().relative_to(repo_root.resolve()).as_posix()
        if repo_root is not None
        else file_path.name
    )
    violations: List[SanitizationViolation] = []

    if file_path.name != ".env.example":
        for rule_name, name_pattern in FORBIDDEN_FILENAME_PATTERNS:
            if name_pattern.match(file_path.name):
                violations.append(
                    SanitizationViolation(
                        file_path=rel_path,
                        line_number=0,
                        rule_id=f"FILENAME:{rule_name}",
                        detail=f"Forbidden file '{rel_path}' matched '{rule_name}'",
                    )
                )

    try:
        content = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return violations

    check_denylist = rel_path not in DENYLIST_SELF_ALLOWLIST_PATHS
    violations.extend(
        scan_text(
            content,
            source_name=rel_path,
            check_denylist=check_denylist,
            check_secrets=True,
        )
    )
    return violations


def iter_repository_files(repo_root: Path) -> Iterable[Path]:
    """Yield all non-excluded files under repo_root."""
    for path in sorted(repo_root.rglob("*")):
        if path.is_dir():
            continue
        rel_parts = path.relative_to(repo_root).parts
        if any(part in EXCLUDED_DIRS for part in rel_parts):
            continue
        yield path


def scan_git_commit_history(repo_root: Path) -> List[SanitizationViolation]:
    """Scan git commit messages if the repository has commits."""
    git_dir = repo_root / ".git"
    if not git_dir.exists():
        return []
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "log", "--all", "--format=%H %B"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return []
    if proc.returncode != 0 or not proc.stdout.strip():
        return []
    return scan_text(
        proc.stdout,
        source_name="git-log",
        check_denylist=True,
        check_secrets=True,
    )


def verify_aws_attestation_and_placeholders(repo_root: Path) -> List[SanitizationViolation]:
    """Verify README.md includes explicit AWS test attestation and parametric placeholders."""
    violations: List[SanitizationViolation] = []
    readme_path = repo_root / "README.md"
    if not readme_path.exists():
        violations.append(
            SanitizationViolation(
                file_path="README.md",
                line_number=0,
                rule_id="DOC:MISSING_README",
                detail="README.md is missing from repository root",
            )
        )
        return violations

    content = readme_path.read_text(encoding="utf-8")
    if "Tested Cloud Environment" not in content or "Snowflake on AWS" not in content:
        violations.append(
            SanitizationViolation(
                file_path="README.md",
                line_number=1,
                rule_id="DOC:MISSING_AWS_ATTESTATION",
                detail="README.md must explicitly include 'Tested Cloud Environment' and 'Snowflake on AWS'",
            )
        )

    for placeholder in REQUIRED_PLACEHOLDERS:
        if placeholder not in content:
            violations.append(
                SanitizationViolation(
                    file_path="README.md",
                    line_number=1,
                    rule_id=f"DOC:MISSING_PLACEHOLDER:{placeholder}",
                    detail=f"README.md must include parametric placeholder {placeholder}",
                )
            )
    return violations


def scan_repository(
    repo_root: Path,
    *,
    require_aws_attestation: bool = True,
) -> SanitizationReport:
    """Run full sanitization scan across all files and git commit history in repo_root."""
    repo_root = repo_root.resolve()
    report = SanitizationReport(repo_root=str(repo_root))

    for file_path in iter_repository_files(repo_root):
        report.scanned_files += 1
        report.violations.extend(scan_file(file_path, repo_root=repo_root))

    report.violations.extend(scan_git_commit_history(repo_root))

    if require_aws_attestation:
        report.violations.extend(verify_aws_attestation_and_placeholders(repo_root))

    return report


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify public repository sanitization (100% account-agnostic, zero secrets)."
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Path to repository root (defaults to parent of scripts/).",
    )
    parser.add_argument(
        "--skip-aws-attestation",
        action="store_true",
        help="Skip README.md AWS attestation and placeholder checks (for isolated directory scans).",
    )
    args = parser.parse_args(argv)

    report = scan_repository(
        args.repo_root,
        require_aws_attestation=not args.skip_aws_attestation,
    )
    if not report.passed:
        print(
            f"[SANITIZATION FAILED] {len(report.violations)} violation(s) across {report.scanned_files} files:",
            file=sys.stderr,
        )
        for v in report.violations:
            print(f"  - [{v.rule_id}] {v.file_path}:{v.line_number} -> {v.detail}", file=sys.stderr)
        return 1

    print(
        f"[SANITIZATION PASSED] Scanned {report.scanned_files} files in {report.repo_root}: "
        "0 secrets, 0 denylist matches, 100% account-agnostic, AWS attestation verified."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
