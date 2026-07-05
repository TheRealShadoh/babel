#!/bin/sh
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

if [ "$(id -u)" = "0" ]; then
    if [ "$(id -g babel)" != "$PGID" ]; then
        groupmod -o -g "$PGID" babel
    fi
    if [ "$(id -u babel)" != "$PUID" ]; then
        usermod -o -u "$PUID" babel
    fi
    chown -R babel:babel /app/data
    exec gosu babel "$@"
fi

exec "$@"
