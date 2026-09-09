# Stage 1: 解压 N_m3u8DL-RE 二进制（用 alpine，体积小，结果不进最终镜像）
FROM alpine:3.19 AS extractor

ARG TARGETARCH
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
    cp "$RE_BIN" /tmp/N_m3u8DL-RE; \
    chmod 755 /tmp/N_m3u8DL-RE; \
    ls -la /tmp/N_m3u8DL-RE

# Stage 2: 最终镜像
FROM python:3.11-slim

ENV TZ=Asia/Shanghai
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg gosu tzdata && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 只从 extractor 拿解压后的单个二进制，vendor tar 包不进最终镜像（省 ~16MB 层）
COPY --from=extractor /tmp/N_m3u8DL-RE /app/N_m3u8DL-RE

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
