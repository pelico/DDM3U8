FROM python:3.11-slim

ENV TZ=Asia/Shanghai
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg tzdata su-exec shadow && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir yt-dlp -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY templates ./templates
COPY main.py .

RUN printf '%s\n' '#!/bin/sh' \
    'PUID=${PUID:-0}' \
    'PGID=${PGID:-0}' \
    'if [ "$PUID" != "0" ] || [ "$PGID" != "0" ]; then' \
    '    echo "Setting up user with PUID=$PUID, PGID=$PGID"' \
    '    if ! getent group appuser > /dev/null 2>&1; then' \
    '        groupadd -g "$PGID" appuser' \
    '    else' \
    '        groupmod -g "$PGID" appuser' \
    '    fi' \
    '    if ! id appuser > /dev/null 2>&1; then' \
    '        useradd -u "$PUID" -g "$PGID" -d /app appuser' \
    '    else' \
    '        usermod -u "$PUID" -g "$PGID" appuser' \
    '    fi' \
    '    chown -R "$PUID:$PGID" /downloads' \
    'fi' \
    'echo "Starting DDM3U8 service..."' \
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