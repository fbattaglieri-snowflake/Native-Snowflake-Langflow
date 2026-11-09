"""W3-R1 Phase 2: release image pins the upstream Langflow base and keeps private files out."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_dockerfile_release_pins_base_and_runs_installer() -> None:
    text = (ROOT / "Dockerfile.release").read_text()
    assert "ARG LANGFLOW_BASE=langflowai/langflow:1.13.2" in text
    assert "FROM --platform=linux/amd64 ${LANGFLOW_BASE}" in text
    assert "scripts/install_live_hooks_in_container.py" in text
    assert text.rstrip().splitlines()[-3:] == ["USER user", "WORKDIR /app", 'CMD ["langflow", "run"]']


def test_dockerignore_excludes_private_material() -> None:
    lines = set((ROOT / ".dockerignore").read_text().splitlines())
    for entry in (".git", "tests", "*.md", "**/__pycache__", ".env", "**/*.p8", "**/*.pem", "**/*.db"):
        assert entry in lines
