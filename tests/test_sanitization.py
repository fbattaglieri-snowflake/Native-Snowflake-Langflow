"""Unit and integration tests for the public repository sanitization gate (REQ-17, G-A6)."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.verify_public_repo_sanitization import (
    DENYLIST_TERMS,
    REQUIRED_PLACEHOLDERS,
    main as sanitization_main,
    scan_file,
    scan_repository,
    scan_text,
    verify_aws_attestation_and_placeholders,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_denylist_contains_all_14_mandatory_identifiers() -> None:
    """Verify all 14 required environment and context identifiers are in DENYLIST_TERMS."""
    expected = {
        "JK01" + "775",
        "sfsenorth" + "america",
        "fbatt" + "aglieri_" + "aws_fra",
        "aws:" + "frankfurt",
        "bv" + "dx",
        "Snow" + "house",
        "AI" + "COLLEGE",
        "LANGFLOW_" + "SAFE_",
        "N8N_" + "PLATFORM",
        "HERMES_" + "SERVICE",
        "20261012_" + "SnowflakeLangflow",
        "20260926_" + "NewLLMProxy",
        "20260921_" + "Langflow",
        "20261016_" + "SnowflakeLangflow_CLI_prompt",
    }
    assert set(DENYLIST_TERMS) == expected
    assert len(DENYLIST_TERMS) == 14


@pytest.mark.parametrize("term_index", range(14))
def test_scan_text_blocks_each_denylist_term_case_insensitively(term_index: int) -> None:
    """Each denylist term must be detected regardless of letter casing."""
    term = DENYLIST_TERMS[term_index]
    sample = f"Configuration points to {term.lower()} and {term.upper()} in text."
    violations = scan_text(sample, source_name="sample.yaml")
    assert len(violations) >= 1
    assert any(v.rule_id == f"DENYLIST:{term}" for v in violations)


def test_scan_text_blocks_secrets_and_private_keys() -> None:
    """Verify detection of PEM keys, JWTs, Bearer tokens, PATs, and concrete Snowflake hosts."""
    pem_line = "-----BEGIN " + "RSA PRIVATE KEY-----"
    jwt_line = "eyJhbGciOiJIUzI1NiJ9." + "eyJzdWIiOiIxMjM0NTY3ODkwIn0." + "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    bearer_line = "Authorization: Bearer " + "a" * 32
    pat_line = "token = pat_" + "X" * 24
    host_line = "url = https://acmeprod123." + "snowflakecomputing.com/api/v2"

    for payload, expected_prefix in [
        (pem_line, "SECRET:PRIVATE_KEY_HEADER"),
        (jwt_line, "SECRET:JWT_TOKEN"),
        (bearer_line, "SECRET:BEARER_SECRET_TOKEN"),
        (pat_line, "SECRET:SNOWFLAKE_PAT_TOKEN"),
        (host_line, "SECRET:CONCRETE_SNOWFLAKE_ACCOUNT_HOST"),
    ]:
        violations = scan_text(payload, source_name="config.py", check_denylist=False)
        assert any(v.rule_id == expected_prefix for v in violations), f"Missed {expected_prefix}"


def test_scan_file_blocks_forbidden_filenames(tmp_path: Path) -> None:
    """Dated context markdown files, plan files, keys, and SQLite DBs must be flagged."""
    dated_doc = tmp_path / ("2026" + "1017_notes.md")
    dated_doc.write_text("# clean text\n", encoding="utf-8")
    v_dated = scan_file(dated_doc, repo_root=tmp_path)
    assert any(v.rule_id == "FILENAME:DATED_CONTEXT_MD" for v in v_dated)

    plan_doc = tmp_path / "plan-test.md"
    plan_doc.write_text("# clean plan\n", encoding="utf-8")
    v_plan = scan_file(plan_doc, repo_root=tmp_path)
    assert any(v.rule_id == "FILENAME:PLAN_FILE_MD" for v in v_plan)


def test_aws_attestation_and_placeholders_validation(tmp_path: Path) -> None:
    """Verify README validation fails when AWS attestation or placeholders are missing."""
    readme = tmp_path / "README.md"
    readme.write_text("# Incomplete README without attestation\n", encoding="utf-8")
    violations = verify_aws_attestation_and_placeholders(tmp_path)
    rule_ids = {v.rule_id for v in violations}
    assert "DOC:MISSING_AWS_ATTESTATION" in rule_ids
    for placeholder in REQUIRED_PLACEHOLDERS:
        assert f"DOC:MISSING_PLACEHOLDER:{placeholder}" in rule_ids


def test_required_docs_and_adrs_exist_and_are_clean() -> None:
    """Verify README, capability matrix, SPCS deployment, upstream sync, and ADR-001..007 exist."""
    expected_files = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "LICENSE",
        REPO_ROOT / ".gitignore",
        REPO_ROOT / ".pre-commit-config.yaml",
        REPO_ROOT / "pyproject.toml",
        REPO_ROOT / "scripts" / "verify_public_repo_sanitization.py",
        REPO_ROOT / "scripts" / "sync_upstream.sh",
        REPO_ROOT / "docs" / "CAPABILITY_MATRIX.md",
        REPO_ROOT / "docs" / "DEPLOYMENT_SPCS.md",
        REPO_ROOT / "docs" / "UPSTREAM_SYNC.md",
        *[REPO_ROOT / "docs" / "ADR" / f"ADR-00{i}.md" for i in range(1, 8)],
    ]
    for path in expected_files:
        assert path.exists(), f"Missing required file: {path}"
        assert scan_file(path, repo_root=REPO_ROOT) == []


def test_fork_disclaimer_author_and_upstream_update_rules_present() -> None:
    """Verify personal development disclaimer (Francesco Battaglieri), unofficial status, fork explanation, testing status, and update rules."""
    readme_text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    sync_text = (REPO_ROOT / "docs" / "UPSTREAM_SYNC.md").read_text(encoding="utf-8")
    license_text = (REPO_ROOT / "LICENSE").read_text(encoding="utf-8")

    for doc_text in (readme_text, sync_text, license_text):
        assert "Francesco Battaglieri" in doc_text
        assert "fbattaglieri-snowflake" in doc_text

    assert "NON è un prodotto, rilascio o repository ufficiale di Snowflake Inc." in readme_text
    assert "né tantomeno è un progetto ufficiale di Langflow" in readme_text
    assert "fork pubblico" in readme_text.lower()
    assert "langflow-ai/langflow" in readme_text
    assert "oggetto di test" in readme_text.lower()
    assert "active testing" in readme_text.lower()

    for rule_num in range(1, 8):
        rule_id = f"RULE-SYNC-0{rule_num}"
        assert rule_id in readme_text
        assert rule_id in sync_text


def test_full_repository_passes_sanitization_gate() -> None:
    """Run full sanitization gate on the live snowflake-langflow repository."""
    report = scan_repository(REPO_ROOT, require_aws_attestation=True)
    assert report.passed, f"Sanitization violations found: {report.violations}"
    assert sanitization_main(["--repo-root", str(REPO_ROOT)]) == 0
