# DDM3U8 (go-core)

DDM3U8 是一个轻量的 Web M3U8 下载工具。`go-core` 分支是 Go 重写版：用 Go + [uTLS](https://github.com/refraction-networking/utls) 替换原 Python/Flask + yt-dlp 实现，提供原生浏览器 TLS 指纹伪装，体积更小、启动更快、不依赖 Python 运行时。Web 端 API 与原 Flask 版兼容，前端页面可继续复用。

当前镜像路线：

- 基础运行时：`alpine:3.20` + `ffmpeg` + 静态 Go 二进制
- 下载核心：自研 Go 下载器（uTLS 指纹伪装 + HTTP/2 多路复用）
- 合并工具：`ffmpeg`
- 支持架构：`linux/amd64`、`linux/arm64`、`linux/armv7l`

| 架构 | 镜像标签 | TLS 指纹伪装 | 说明 |
|------|----------|--------------|------|
| amd64 | `go-web-amd64` | ✅ uTLS Chrome 133 / Safari / Firefox | 完整功能，可下 TLS 指纹 CDN |
| arm64 | `go-web-arm64` | ✅ uTLS Chrome 133 / Safari / Firefox | 完整功能，可下 TLS 指纹 CDN |
| armv7l | `go-web-armv7l` | ✅ uTLS Chrome 133 / Safari / Firefox | 完整功能，可下 TLS 指纹 CDN（已用 Go 重写，不再受 PyPI wheel 限制） |

> `go-core` 分支已用 Go 重写所有下载逻辑，armv7l 也能开 uTLS 指纹伪装，不再像原 Python 版那样退化。

## 功能

- Web 页面提交 M3U8 下载任务
- 支持全局 Referer / Origin / Cookie / 自定义请求头
- uTLS 模拟浏览器 ClientHello 指纹，绕过 Cloudflare 等 CDN 检测
- HTTP/2 多路复用：一条 TLS 连接并发拉多分片，行为贴近真实浏览器
- 并发分片下载（默认 3 路，避免瞬时突发连接触发 CDN bot 风控）
- 断点续传：恢复任务时自动跳过已下完的分片
- 失败分片可手动恢复 / 强合
- AES-128-CBC 分片解密（支持 key 轮换 warn）
- 两步下载：分片下载 → ffmpeg concat 封装为 mp4
- 任务历史持久化（JSON 文件存到 `/downloads` 卷里，重启不丢）
- 启动时自动清理无任务对应的孤儿 `_temp` 目录
- 支持 Basic Auth 访问保护
- 容器重启后自动恢复任务
- 主动掐流量识别：连续 3 次 < 30s 快速失败 → 后续分片 timeout 自动翻倍（60s→120s→240s，封顶 300s），给 CDN 一个"等流量过去"的窗口
- 分片耗时摘要：任务结束时输出成功/失败分片的耗时分布，辅助判断网络状况

## 快速运行

### amd64 / arm64 架构

```bash
# 任选一个 tag（amd64 和 arm64 用同样的 tag，Docker 会按本机架构自动拉对应镜像）
docker pull ghcr.io/pelico/ddm3u8:go-web-amd64
# 或带分支后缀的版本
docker pull ghcr.io/pelico/ddm3u8:go-web-amd64-go-core
```

```bash
docker run -d \
  --name ddm3u8 \
  -p 8080:8080 \
  -v ./downloads:/downloads \
  -e TZ=Asia/Shanghai \
  -e WEB_USER=admin \
  -e WEB_PASS=admin \
  --restart unless-stopped \
  ghcr.io/pelico/ddm3u8:go-web-amd64
```

### armv7l 架构

```bash
docker pull ghcr.io/pelico/ddm3u8:go-web-armv7l
```

```bash
docker run -d \
  --name ddm3u8 \
  -p 8080:8080 \
  -v ./downloads:/downloads \
  -e TZ=Asia/Shanghai \
  -e MAX_DOWNLOADS=2 \
  -e WEB_USER=admin \
  -e WEB_PASS=admin \
  --restart unless-stopped \
  ghcr.io/pelico/ddm3u8:go-web-armv7l
```

> **armv7l 建议**：`MAX_DOWNLOADS=2` 适应 ARM 设备性能。

### 群晖 Docker Compose

在 `/volume1/docker/ddm3u8/` 下保存为 `docker-compose.yml`：

```yaml
services:
  ddm3u8:
    image: ghcr.io/pelico/ddm3u8:go-web-amd64   # arm64 改 :go-web-arm64，armv7l 改 :go-web-armv7l
    container_name: ddm3u8
    restart: unless-stopped
    ports:
      - "8080:8080"
    volumes:
      - /volume1/docker/ddm3u8/downloads:/downloads
    environment:
      - WEB_USER=
      - WEB_PASS=
      - MAX_DOWNLOADS=3
      - FINGERPRINT=chrome
    logging:
      driver: json-file
      options:
        max-size: "20m"
        max-file: "3"
```

```bash
cd /volume1/docker/ddm3u8
sudo docker compose up -d
```

浏览器访问：

```text
http://你的服务器IP:8080/
```

如果设置了 `WEB_USER` 和 `WEB_PASS`，浏览器会弹出登录框。

查看日志：

```bash
docker logs -f ddm3u8
```

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `PORT` | `8080` | Web 服务监听端口 |
| `MAX_DOWNLOADS` | `3` | 最大并发下载数（同时跑的任务数） |
| `DOWNLOAD_DIR` | `/downloads` | 下载保存目录 |
| `FFMPEG_PATH` | `ffmpeg` | ffmpeg 二进制路径 |
| `FINGERPRINT` | `chrome` | TLS 指纹：`chrome` / `safari` / `firefox` |
| `DB_PATH` | `/downloads/tasks_history.json` | 任务历史 JSON 持久化路径（空则不持久化） |
| `TZ` | `Asia/Shanghai` | 容器时区 |
| `WEB_USER` | 空 | Web 登录用户名，留空则不启用鉴权 |
| `WEB_PASS` | 空 | Web 登录密码，留空则不启用鉴权 |

## 健康检查

基础健康检查：

```bash
curl -u admin:admin http://localhost:8080/health
```

服务就绪检查：

```bash
curl -u admin:admin http://localhost:8080/ready
```

`/ready` 会返回 ffmpeg、下载目录、任务记录加载状态。

## CLI 模式

容器内自带 `ddm3u8-go` 二进制，可直接用于单任务下载：

```bash
docker run --rm \
  -v ./downloads:/downloads \
  --entrypoint ddm3u8-go \
  ghcr.io/pelico/ddm3u8:go-web-amd64 \
  -url 'https://example.com/path/to/playlist.m3u8' \
  -save-dir /downloads \
  -save-name my_video \
  -concurrency 5 \
  -referer 'https://example.com/' \
  -fingerprint chrome
```

常用参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `-url` | （必填） | m3u8 URL |
| `-save-dir` | `.` | 保存目录 |
| `-save-name` | 时间戳 | 输出文件名（不含扩展名） |
| `-temp-dir` | `save-dir/<name>_temp` | 临时分片目录 |
| `-concurrency` | `10` | 并发分片数（Web 默认 3） |
| `-retries` | `10` | 单分片最大重试次数 |
| `-timeout` | `60` | 单分片超时（秒） |
| `-fingerprint` | `chrome` | TLS 指纹：`chrome` / `safari` / `firefox` |
| `-referer` | 空 | Referer header |
| `-user-agent` | 默认 Chrome | User-Agent header |
| `-insecure` | `false` | 跳过 TLS 证书校验 |
| `-ffmpeg` | `ffmpeg` | ffmpeg 路径 |
| `-no-merge` | `false` | 跳过 ffmpeg 合并，仅保留分片 |
| `-v` | `false` | 详细日志（含每分片进度） |

## 下载技巧

- **手机浏览器能播但下载失败**：多半是 CDN 检测 TLS 指纹或 Referer。在高级选项里填 Referer（如 `https://missav.fans`），镜像默认会启用 uTLS Chrome 133 模拟真实 Chrome 浏览器。
- **指纹选择**：默认 Chrome 133 对大多数 CDN 效果最好；如果某个站点针对性屏蔽 Chrome UA，可试 Safari (`-fingerprint safari`) 或 Firefox (`-fingerprint firefox`)。
- **下载中断后**：任务列表里点"强合"可拼接已下载的 `.ts` 碎片。
- **单分片 timeout**：默认 60s。遇到慢源时连续 3 次 < 30s 快速失败会自动把 timeout 翻倍到 120s/240s（封顶 300s），给 CDN 一个"等流量过去"的窗口。
- **分片耗时摘要**：任务完成后日志区会输出 `[分片耗时摘要]` 和 `[失败分片]` 两行，可据此判断是慢网还是被 CDN 主动掐流量（快速失败 < 30s 占比 ≥ 70% 高度疑似主动掐）。

## 注意事项

- Go 1.25 编译，静态二进制无 glibc 依赖；交叉编译三架构（amd64 / arm64 / armv7l）由 CI workflow 自动构建并发布。
- `go-core` 分支只发布 `go-web-*` 镜像标签，不再维护 `latest` 标签。原 Python/Flask 版的 `latest` / `armv7l` 标签保留在 `main` 分支。
- API 与原 Flask 版兼容：路径、参数、返回 JSON 结构一致，方便前端在不同后端间切换。
- 镜像内同时打包了 `ddm3u8-go`（CLI）和 `ddm3u8-web`（Web 服务），默认 entrypoint 跑 Web；想用 CLI 模式加 `--entrypoint ddm3u8-go` 即可。
- 不建议暴露到公网；如果必须公网访问，请放在反向代理后面并启用 HTTPS。
