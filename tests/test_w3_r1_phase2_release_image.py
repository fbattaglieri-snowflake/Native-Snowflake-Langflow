"""W3-R1 Phase 2: release image pins the upstream Langflow base and keeps private files out."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_dockerfile_release_pins_base_and_runs_installer() -> None:
    text = (ROOT / "Dockerfile.release").read_text()
    assert "ARG LANGFLOW_BASE=langflowai/langflow:1.13.1@sha256:643d78228cd148be70e52a860dd3456c834a070c98c81b354f713908d40949f8" in text
    assert "FROM --platform=linux/amd64 ${LANGFLOW_BASE}" in text
    assert "scripts/install_live_hooks_in_container.py" in text
    assert text.rstrip().splitlines()[-3:] == ["USER user", "WORKDIR /app", 'CMD ["langflow", "run"]']


def test_dockerignore_excludes_private_material() -> None:
    lines = set((ROOT / ".dockerignore").read_text().splitlines())
    for entry in (".git", "tests", "*.md", "**/__pycache__", ".env", "**/*.p8", "**/*.pem", "**/*.db"):
        assert entry in lines


def test_release_image_build_state_is_throwaway_and_deleted_same_layer():
    text = (ROOT / "Dockerfile.release").read_text()
    run = text[text.index("install_live_hooks_in_container.py \\") - 600:text.index("ENV PYTHONPATH")]
    assert 'BUILD_STATE="$(mktemp -d)"' in run
    for env in ("XDG_CACHE_HOME=\"$BUILD_STATE", "LANGFLOW_CONFIG_DIR=\"$BUILD_STATE", "LANGFLOW_DATABASE_URL=\"sqlite:///$BUILD_STATE"):
        assert env in run
    assert 'rm -rf "$BUILD_STATE" /tmp/.cache' in run
    for name in ("secret_key", "langflow.db-wal", "langflow.db-shm"):
        assert name in run
    assert "build state leaked into image" in run
    assert "chown -R user /persist /tmp/uv-cache /tmp/.cache" in run
    assert run.count("RUN ") == 1
