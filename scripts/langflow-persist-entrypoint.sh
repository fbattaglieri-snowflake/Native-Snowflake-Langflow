#!/bin/sh
# Langflow entrypoint with persistent block storage.
#
# Why it exists: SPCS mounts block volumes owned by root, and the spec uid/gid
# fields only apply to stage volumes. Langflow runs as uid 1000, so without this
# step it could not write to the volume, and the failure would show up on the
# first write instead of at startup.
#
# The container starts as root only to chown the mount, then setpriv drops
# privileges: Langflow never runs as root.
set -eu

DATA_DIR="${LANGFLOW_PERSIST_DIR:-/persist}"

if [ "$(id -u)" = "0" ]; then
    mkdir -p "$DATA_DIR/config" "$DATA_DIR/storage"
    # Only the mount, never the application: the code in /app stays immutable.
    chown -R 1000:0 "$DATA_DIR"
    chmod 0770 "$DATA_DIR"
    exec setpriv --reuid=1000 --regid=0 --init-groups -- "$0" "$@"
fi

# From here on: uid 1000.
if [ ! -w "$DATA_DIR" ]; then
    echo "ERROR: $DATA_DIR is not writable by uid $(id -u); the volume would not persist" >&2
    exit 1
fi

export LANGFLOW_CONFIG_DIR="$DATA_DIR/config"
# SQLite on the volume: this is what survives restarts and suspend/resume.
export LANGFLOW_DATABASE_URL="${LANGFLOW_DATABASE_URL:-sqlite:///$DATA_DIR/langflow.db}"

echo "persistence: $DATA_DIR (uid $(id -u)), database on block volume"
exec langflow run "$@"
