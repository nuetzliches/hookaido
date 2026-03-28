#!/bin/sh
set -e

# If running as root, fix volume ownership and drop to app user.
if [ "$(id -u)" = '0' ]; then
    mkdir -p /app/.data
    chown -R hookaido:hookaido /app/.data
    exec su-exec hookaido "$0" "$@"
fi

exec hookaido "$@"
