#!/bin/sh
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

case "$PUID$PGID" in
    *[!0-9]*) echo "babel: PUID/PGID must be numeric (got PUID=$PUID PGID=$PGID)" >&2; exit 1 ;;
esac

if [ "$(id -u)" = "0" ]; then
    if [ "$(id -g babel)" != "$PGID" ]; then
        groupmod -o -g "$PGID" babel
    fi
    if [ "$(id -u babel)" != "$PUID" ]; then
        usermod -o -u "$PUID" babel
    fi
    # A bind mount on root-squashed NFS or a read-only volume refuses chown;
    # that is not a reason to refuse to start. Permission problems will show
    # up as a clear database error instead of a container that exits silently.
    if ! chown -R babel:babel /app/data 2>/dev/null; then
        echo "babel: could not chown /app/data to ${PUID}:${PGID}; continuing" >&2
    fi
    exec gosu babel "$@"
fi

exec "$@"
