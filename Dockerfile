FROM python:3.11-alpine

ENV TZ=Asia/Shanghai
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apk add --no-cache ffmpeg su-exec tini tzdata bash

COPY requirements.txt .
RUN pip install --no-cache-dir --retries 3 --timeout 60 -r requirements.txt

COPY templates ./templates
COPY main.py .

RUN printf '%s\n' '#!/bin/sh' \
    'PUID=${PUID:-0}' \
    'PGID=${PGID:-0}' \
    'if [ "$PUID" != "0" ] || [ "$PGID" != "0" ]; then' \
    '    echo "Setting up user with PUID=$PUID, PGID=$PGID"' \
    '    addgroup -g "$PGID" appuser 2>/dev/null || true' \
    '    adduser -D -u "$PUID" -G appuser -h /app appuser 2>/dev/null || true' \
    '    chown -R "$PUID:$PGID" /downloads' \
    'fi' \
    'echo "Starting DDM3U8 service (yt-dlp)..."' \
    'if [ "$PUID" != "0" ] || [ "$PGID" != "0" ]; then' \
    '    exec su-exec appuser python main.py' \
    'else' \
    '    exec python main.py' \
    'fi' > /entrypoint.sh && \
    chmod +x /entrypoint.sh && \
    mkdir -p /downloads

VOLUME ["/downloads"]
EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
