# DDM3U8

DDM3U8 是一个轻量的 Web M3U8 下载工具，基于 Flask 和 N_m3u8DL-RE。它提供网页任务管理、下载目录浏览、本地合并、任务状态轮询和基础鉴权，适合部署在 NAS 或家庭服务器上长期运行。

当前镜像路线：

- 基础镜像：`python:3.11-alpine`
- 下载核心：`N_m3u8DL-RE`
- 合并工具：`ffmpeg`
- 支持架构：`linux/amd64`、`linux/arm64`
- 不支持：`armv7l`

## 功能

- Web 页面提交 M3U8 下载任务
- 支持 Basic Auth 访问保护
- 支持并发下载数量限制
- 下载任务状态自动刷新
- 支持 `/downloads` 持久化保存
- 支持重启后恢复历史任务记录
- 内置 `/health` 和 `/ready` 探针
- 镜像内置 `ffmpeg`
- 镜像构建时从 `vendor/` 选择对应架构的 `N_m3u8DL-RE`

## 目录结构

```text
.
├── Dockerfile
├── entrypoint.sh
├── main.py
├── requirements.txt
├── templates/
│   └── index.html
├── vendor/
│   ├── N_m3u8DL-RE_v0.5.1-beta_linux-musl-arm64_20251029.tar.gz
│   └── N_m3u8DL-RE_v0.5.1-beta_linux-musl-x64_20251029.tar.gz
└── .env.example
```

`vendor/` 目录必须保留。Docker 构建时会根据目标架构选择对应的 N_m3u8DL-RE 包。

## 快速运行

如果你已经有镜像，可以直接运行：

```bash
docker run -d \
  --name ddm3u8 \
  -p 8080:8080 \
  -v /volume1/docker/dd3/downloads:/downloads \
  -e TZ=Asia/Shanghai \
  -e WEB_USER=admin \
  -e WEB_PASS=admin \
  --restart unless-stopped \
  ghcr.io/你的GitHub用户名/ddm3u8:latest
```

浏览器访问：

```text
http://你的服务器IP:8080/
```

如果设置了 `WEB_USER` 和 `WEB_PASS`，浏览器会弹出登录框。

## 本地构建

在普通 x86_64 NAS 或服务器上，可以使用传统 Docker 构建：

```bash
docker build -t ddm3u8:test .
```

运行：

```bash
docker run -d \
  --name ddm3u8 \
  -p 8080:8080 \
  -v /volume1/docker/dd3/downloads:/downloads \
  -e TZ=Asia/Shanghai \
  -e WEB_USER=admin \
  -e WEB_PASS=admin \
  --restart unless-stopped \
  ddm3u8:test
```

查看日志：

```bash
docker logs -f ddm3u8
```

## 多架构发布

项目内置 GitHub Actions 工作流，会在以下情况构建并发布镜像：

- 推送到 `main` 分支
- 推送版本标签，例如 `v0.1.0`
- 在 GitHub 页面手动运行工作流

默认发布到 GitHub Container Registry：

```text
ghcr.io/你的GitHub用户名/ddm3u8:latest
ghcr.io/你的GitHub用户名/ddm3u8:v0.1.0
```

支持平台：

```text
linux/amd64
linux/arm64
```

首次发布后，如果镜像无法公开拉取，需要到 GitHub 仓库的 Packages 页面，把容器包可见性改成 Public。

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `PORT` | `8080` | Web 服务监听端口 |
| `MAX_DOWNLOADS` | `2` | 最大并发下载数 |
| `TZ` | `Asia/Shanghai` | 容器时区 |
| `WEB_USER` | 空 | Web 登录用户名，留空则不启用鉴权 |
| `WEB_PASS` | 空 | Web 登录密码，留空则不启用鉴权 |
| `PUID` | `0` | 容器内运行用户 UID |
| `PGID` | `0` | 容器内运行用户 GID |

群晖上如果遇到下载目录权限问题，可以设置 `PUID` 和 `PGID` 为宿主机对应用户的 ID。

## 健康检查

基础健康检查：

```bash
curl -u admin:admin http://localhost:8080/health
```

服务就绪检查：

```bash
curl -u admin:admin http://localhost:8080/ready
```

`/ready` 会返回 ffmpeg、N_m3u8DL-RE、下载目录、任务记录加载状态。

## 群晖部署示例

```bash
docker run -d \
  --name ddm3u8 \
  -p 8080:8080 \
  -v /volume1/docker/dd3/downloads:/downloads \
  -e TZ=Asia/Shanghai \
  -e WEB_USER=admin \
  -e WEB_PASS=请改成强密码 \
  --restart unless-stopped \
  ghcr.io/你的GitHub用户名/ddm3u8:latest
```

更新镜像：

```bash
docker pull ghcr.io/你的GitHub用户名/ddm3u8:latest
docker stop ddm3u8
docker rm ddm3u8
docker run -d \
  --name ddm3u8 \
  -p 8080:8080 \
  -v /volume1/docker/dd3/downloads:/downloads \
  -e TZ=Asia/Shanghai \
  -e WEB_USER=admin \
  -e WEB_PASS=请改成强密码 \
  --restart unless-stopped \
  ghcr.io/你的GitHub用户名/ddm3u8:latest
```

## 注意事项

- `vendor/` 里的两个压缩包必须上传到 GitHub，否则 GitHub Actions 构建会失败。
- `entrypoint.sh` 必须保持 Linux LF 换行；Dockerfile 已在构建时自动去除 CRLF。
- Flask 当前使用内置开发服务器，适合个人 NAS 和内网环境使用。
- 不建议暴露到公网；如果必须公网访问，请放在反向代理后面并启用 HTTPS。
