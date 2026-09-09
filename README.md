# DDM3U8

DDM3U8 是一个轻量的 Web M3U8 下载工具，基于 Flask 和 yt-dlp。它提供网页任务管理、下载目录浏览、本地合并、任务状态轮询和基础鉴权，适合部署在 NAS 或家庭服务器上长期运行。

当前镜像路线：

- 基础镜像：`python:3.11-alpine`
- 下载核心：`yt-dlp`（amd64 / arm64 启用 TLS 指纹伪装，armv7l 跳过）
- 合并工具：`ffmpeg`
- 支持架构：`linux/amd64`、`linux/arm64`、`linux/armv7l`

| 架构 | 镜像标签 | TLS 指纹伪装 | 说明 |
|------|----------|--------------|------|
| amd64 | `latest` | ✅ `--impersonate chrome` | 完整功能，可下 TLS 指纹 CDN |
| arm64 | `latest` | ✅ `--impersonate chrome` | 完整功能，可下 TLS 指纹 CDN |
| armv7l | `armv7l` | ❌ 跳过 | PyPI 无 armv7l 预编译 wheel，退化为普通下载 |

<img width="1479" height="1300" alt="image" src="https://github.com/user-attachments/assets/e33d424b-e709-46fd-a2f3-d36fba1b8088" />

## 功能

- Web 页面提交 M3U8 下载任务
- 支持全局 Referer / Origin / Cookie / 自定义请求头
- yt-dlp `--impersonate chrome` 模拟浏览器 TLS 指纹，绕过部分 CDN 检测
- 两步下载：yt-dlp 取 `.ts` 分片 → ffmpeg 封装为 `.mp4`
- 支持 Basic Auth 访问保护
- 支持并发下载数量限制
- 下载任务状态自动刷新
- 支持 `/downloads` 保存视频
- 本地缓存合并工具（碎片拼接）
- 音频提取（视频转 m4a）
- 容器重启后自动恢复任务
- 中断任务可手动强合


## 快速运行

### amd64 / arm64 架构

```bash
docker pull ghcr.io/pelico/ddm3u8:latest

# 国内镜像加速
docker pull ghcr.nju.edu.cn/pelico/ddm3u8:latest
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
  ghcr.io/pelico/ddm3u8:latest
```

### armv7l 架构

```bash
docker pull ghcr.io/pelico/ddm3u8:armv7l

# 国内镜像加速
docker pull ghcr.nju.edu.cn/pelico/ddm3u8:armv7l
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
  ghcr.io/pelico/ddm3u8:armv7l
```

> **armv7l 版本说明**：因 PyPI 无 curl_cffi 的 armv7l 预编译 wheel，armv7l 镜像不装 curl_cffi，自动跳过 `--impersonate chrome`。普通 m3u8 仍可正常下载；遇到 TLS 指纹识别的 CDN 会 fallback 到 `Connection reset`。建议将 `MAX_DOWNLOADS` 设为 2 以适应 arm 设备性能。

### 群晖 Docker Compose

在 `/volume1/docker/ddm3u8/` 下保存为 `docker-compose.yml`：

```yaml
services:
  ddm3u8:
    image: ghcr.io/pelico/ddm3u8:latest   # armv7l 机器改为 :armv7l
    container_name: ddm3u8
    restart: unless-stopped
    ports:
      - "8080:8080"
    volumes:
      - /volume1/docker/ddm3u8/downloads:/downloads
    environment:
      - PUID=0
      - PGID=0
      - WEB_USER=
      - WEB_PASS=
      - MAX_DOWNLOADS=3
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
| `MAX_DOWNLOADS` | `3` | 最大并发下载数 |
| `TZ` | `Asia/Shanghai` | 容器时区 |
| `WEB_USER` | 空 | Web 登录用户名，留空则不启用鉴权 |
| `WEB_PASS` | 空 | Web 登录密码，留空则不启用鉴权 |
| `PUID` | `0` | 容器内运行用户 UID |
| `PGID` | `0` | 容器内运行用户 GID |

群晖上如果遇到下载目录权限问题，可以设置 `PUID` 和 `PGID` 为宿主机对应用户的 ID（DSM 控制面板 > 共享文件夹 > 权限 查看，或 SSH 用 `id` 命令查询）。


## 健康检查

基础健康检查：

```bash
curl -u admin:admin http://localhost:8080/health
```

服务就绪检查：

```bash
curl -u admin:admin http://localhost:8080/ready
```

`/ready` 会返回 ffmpeg、下载核心（yt-dlp）、TLS 指纹伪装（curl_cffi）、下载目录、任务记录加载状态。


## 下载技巧

- **手机浏览器能播但下载失败**：多半是 CDN 检测 TLS 指纹或 Referer。在高级选项里填 Referer（如 `https://missav.fans`），amd64/arm64 镜像会自动启用 `--impersonate chrome` 模拟 Chrome 浏览器指纹。
- **伪装分片**：部分站点的分片扩展名伪装成 `.jpeg` 但实际是 MPEG-TS。yt-dlp 用 `--hls-prefer-native` 可识别并下载。
- **下载中断后**：任务列表里点"强合"可拼接已下载的 `.ts` 碎片。


## 注意事项

- yt-dlp 的 `--impersonate` 依赖 `curl_cffi`。yt-dlp 2024.12.23 只兼容 curl_cffi 0.7.0/0.7.1，故 `requirements.txt` 锁定 `curl_cffi>=0.7.0,<0.7.2`。
- `entrypoint.sh` 必须保持 Linux LF 换行；Dockerfile 已在构建时自动去除 CRLF。
- Flask 当前使用内置开发服务器，适合个人 NAS 和内网环境使用。
- 不建议暴露到公网；如果必须公网访问，请放在反向代理后面并启用 HTTPS。
