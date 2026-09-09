FROM python:3.11-slim

ARG TARGETARCH

ENV TZ=Asia/Shanghai
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg gosu tzdata && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY vendor /tmp/vendor

RUN set -eux; \
    ARCH="${TARGETARCH:-$(uname -m)}"; \
    case "$ARCH" in \
      amd64|x86_64) RE_TAR="/tmp/vendor/N_m3u8DL-RE_v0.5.1-beta_linux-x64_20251029.tar.gz" ;; \
      arm64|aarch64) RE_TAR="/tmp/vendor/N_m3u8DL-RE_v0.5.1-beta_linux-arm64_20251029.tar.gz" ;; \
      *) echo "Unsupported arch: $ARCH"; exit 1 ;; \
    esac; \
    mkdir -p /tmp/re_extract; \
    tar -xzf "$RE_TAR" -C /tmp/re_extract; \
    RE_BIN="$(find /tmp/re_extract -type f -name 'N_m3u8DL-RE*' ! -name '*.md' | head -n1)"; \
    if [ -z "$RE_BIN" ]; then echo "ERROR: N_m3u8DL-RE binary not found in tarball"; ls -laR /tmp/re_extract; exit 1; fi; \
    mv "$RE_BIN" /app/N_m3u8DL-RE; \
    chmod 755 /app/N_m3u8DL-RE; \
    ls -la /app/N_m3u8DL-RE; \
    rm -rf /tmp/re_extract /tmp/vendor

COPY templates ./templates
COPY main.py .

RUN printf '%s\n' '#!/bin/sh' \
    'PUID=${PUID:-0}' \
    'PGID=${PGID:-0}' \
    'if [ "$PUID" != "0" ] || [ "$PGID" != "0" ]; then' \
    '    echo "Setting up user with PUID=$PUID, PGID=$PGID"' \
    '    groupadd -g "$PGID" appuser 2>/dev/null || groupmod -g "$PGID" appuser' \
    '    id appuser >/dev/null 2>&1 || useradd -u "$PUID" -g "$PGID" -d /app appuser' \
    '    chown -R "$PUID:$PGID" /downloads' \
    'fi' \
    'echo "Starting DDM3U8 service..."' \
    'if [ "$PUID" != "0" ] || [ "$PGID" != "0" ]; then' \
    '    exec gosu appuser python main.py' \
    'else' \
    '    exec python main.py' \
    'fi' > /entrypoint.sh && \
    chmod +x /entrypoint.sh && \
    mkdir -p /downloads

VOLUME ["/downloads"]
EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
