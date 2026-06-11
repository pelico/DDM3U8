FROM python:3.11-alpine

ARG TARGETARCH

ENV TZ=Asia/Shanghai
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories && \
    apk update && \
    apk add --no-cache ffmpeg tzdata su-exec shadow && \
    rm -rf /var/cache/apk/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY vendor /tmp/vendor

RUN set -eux; \
    ARCH="${TARGETARCH:-$(uname -m)}"; \
    case "$ARCH" in \
      amd64|x86_64) RE_TAR="/tmp/vendor/N_m3u8DL-RE_v0.5.1-beta_linux-musl-x64_20251029.tar.gz" ;; \
      arm64|aarch64) RE_TAR="/tmp/vendor/N_m3u8DL-RE_v0.5.1-beta_linux-musl-arm64_20251029.tar.gz" ;; \
      *) echo "Unsupported arch: $ARCH"; exit 1 ;; \
    esac; \
    mkdir -p /tmp/re_extract; \
    tar -xzf "$RE_TAR" -C /tmp/re_extract; \
    find /tmp/re_extract -name "N_m3u8DL-RE*" ! -name "*.md" -type f -exec mv {} /app/N_m3u8DL-RE \; ; \
    chmod 755 /app/N_m3u8DL-RE; \
    rm -rf /tmp/re_extract /tmp/vendor

COPY templates ./templates
COPY main.py .

COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && \
    chmod +x /entrypoint.sh && \
    mkdir -p /downloads

VOLUME ["/downloads"]

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
