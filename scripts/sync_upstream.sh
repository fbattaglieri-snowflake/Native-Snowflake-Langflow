#!/usr/bin/env bash
# Governed Upstream Synchronization Script for SnowflakeLangflow
# Synchronizes a target upstream tag (default: v1.12.5) from langflow-ai/langflow
# into a dedicated sync branch, verifies patch boundaries, runs pytest + sanitization gate.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-upstream}"
UPSTREAM_URL="${UPSTREAM_URL:-https://github.com/langflow-ai/langflow.git}"
TARGET_TAG="${1:-v1.12.5}"
BASE_BRANCH="${BASE_BRANCH:-snowflake/main}"
SYNC_BRANCH="sync/upstream-${TARGET_TAG}"
DRY_RUN="${DRY_RUN:-true}"

echo "[sync_upstream] Repository root: ${REPO_ROOT}"
echo "[sync_upstream] Target upstream tag: ${TARGET_TAG}"
echo "[sync_upstream] Base branch: ${BASE_BRANCH}"
echo "[sync_upstream] Sync branch: ${SYNC_BRANCH}"

if ! git -C "${REPO_ROOT}" remote get-url "${UPSTREAM_REMOTE}" >/dev/null 2>&1; then
  echo "[sync_upstream] Adding remote ${UPSTREAM_REMOTE} -> ${UPSTREAM_URL}"
  git -C "${REPO_ROOT}" remote add "${UPSTREAM_REMOTE}" "${UPSTREAM_URL}"
fi

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "[sync_upstream] DRY_RUN=true: validating sanitization gate and bundle boundaries without network fetch."
  python3 "${REPO_ROOT}/scripts/verify_public_repo_sanitization.py" --repo-root "${REPO_ROOT}"
  echo "[sync_upstream] Dry-run verification succeeded for target ${TARGET_TAG}."
  exit 0
fi

git -C "${REPO_ROOT}" fetch "${UPSTREAM_REMOTE}" --tags
git -C "${REPO_ROOT}" checkout -B "${SYNC_BRANCH}" "${BASE_BRANCH}"
git -C "${REPO_ROOT}" merge --no-commit --no-ff "${TARGET_TAG}" || {
  echo "[sync_upstream] Merge conflict detected against ${TARGET_TAG}. Review tracked core patches in docs/UPSTREAM_SYNC.md." >&2
  exit 1
}

python3 "${REPO_ROOT}/scripts/verify_public_repo_sanitization.py" --repo-root "${REPO_ROOT}"
pytest "${REPO_ROOT}/tests" -q

echo "[sync_upstream] Merge and verification complete on ${SYNC_BRANCH}. Ready for QA review before merging into ${BASE_BRANCH}."
