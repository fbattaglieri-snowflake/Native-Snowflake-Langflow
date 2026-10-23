# syntax=docker/dockerfile:1.7
# SnowflakeLangflow SPCS Container Image
# Target platform: linux/amd64 (Snowpark Container Services requirement)
# Non-root runtime user: UID 1000 / GID 1000
# Zero runtime public internet downloads; reproducible offline wheels/lockfile

FROM --platform=linux/amd64 python:3.12-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_CACHE_DIR=/tmp/uv-cache \
    XDG_CACHE_HOME=/tmp/.cache \
    SQLITE_TMPDIR=/tmp \
    LANGFLOW_HOST=0.0.0.0 \
    LANGFLOW_PORT=7860 \
    LANGFLOW_BUNDLE_PACKAGES="lfx_snowflake" \
    SNOWFLAKE_UPSTREAM_TAG="v1.12.5"

# Create dedicated non-root user and group (UID 1000 / GID 1000) and writable cache/SQLite WAL dirs (0777)
RUN groupadd --gid 1000 langflow \
    && useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash langflow \
    && mkdir -p /app /app/data/.cache/uv /tmp/uv-cache /tmp/.cache /persist /var/lib/langflow/data /var/lib/langflow/outbox \
    && chmod -R 0777 /tmp/uv-cache /tmp/.cache /app/data /persist \
    && chown -R 1000:1000 /app /persist /var/lib/langflow

WORKDIR /app

# Copy package metadata, Snowflake bundle, scripts, and frontend patches
COPY --chown=1000:1000 src/ /app/src/
COPY --chown=1000:1000 frontend_patches/ /app/frontend_patches/
COPY --chown=1000:1000 scripts/ /app/scripts/
COPY --chown=1000:1000 sql/ /app/sql/

ENV PYTHONPATH="/app/src"

USER 1000:1000

EXPOSE 7860

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python3", "-c", "import lfx_snowflake; assert lfx_snowflake.get_bundle_manifest()['bundle_id'] == 'snowflake'"]

CMD ["python3", "-m", "lfx_snowflake"]
