#!/bin/bash
# Ensure data subdirectories exist and are writable.
# Bind mounts may override image-created dirs with host-owned (root) dirs.
for d in /app/data/dimensions /app/data/stage /app/data/history /app/data/pulse /app/logs; do
    mkdir -p "$d" 2>/dev/null
    # If running as root (local dev), fix ownership for appuser
    if [ "$(id -u)" = "0" ] && id appuser &>/dev/null; then
        chown -R appuser:appuser "$d" 2>/dev/null
    fi
done

# If running as root, drop to appuser for the main process
if [ "$(id -u)" = "0" ] && id appuser &>/dev/null; then
    exec su -s /bin/bash appuser -c "exec $*"
fi

exec "$@"
