#!/bin/sh

PUID=${PUID:-0}
PGID=${PGID:-0}

if [ "$PUID" != "0" ] || [ "$PGID" != "0" ]; then
    echo "Setting up user with PUID=$PUID, PGID=$PGID"

    if ! getent group appuser > /dev/null 2>&1; then
        groupadd -g "$PGID" appuser
    else
        groupmod -g "$PGID" appuser
    fi

    if ! id appuser > /dev/null 2>&1; then
        useradd -u "$PUID" -g "$PGID" -d /app appuser
    else
        usermod -u "$PUID" -g "$PGID" appuser
    fi

    chown -R "$PUID:$PGID" /downloads
fi

echo "Starting DDM3U8 service..."
if [ "$PUID" != "0" ] || [ "$PGID" != "0" ]; then
    exec su-exec appuser python main.py
else
    exec python main.py
fi
