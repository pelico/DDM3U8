// ddm3u8-web 是 ddm3u8-go 的 Web 版本，
// 提供 HTTP API（兼容原 Flask 版）+ 内嵌前端页面。
package main

import (
	"embed"
	"flag"
	"log"
	"os"
	"strconv"

	"github.com/pelico/DDM3U8-go/internal/server"
)

//go:embed templates/index.html
var templatesFS embed.FS

func main() {
	port := flag.String("port", envDefault("PORT", "8080"), "Web 服务端口")
	downloadDir := flag.String("download-dir", envDefault("DOWNLOAD_DIR", "/downloads"), "下载保存目录")
	maxParallel := flag.Int("max-downloads", envIntDefault("MAX_DOWNLOADS", 3), "最大并发下载数")
	ffmpegPath := flag.String("ffmpeg", envDefault("FFMPEG_PATH", "ffmpeg"), "ffmpeg 路径")
	fingerprint := flag.String("fingerprint", "chrome", "TLS 指纹: chrome/safari/firefox")
	webUser := flag.String("web-user", envDefault("WEB_USER", ""), "Basic Auth 用户名")
	webPass := flag.String("web-pass", envDefault("WEB_PASS", ""), "Basic Auth 密码")
	// 任务历史持久化路径，对齐 armv7l 的 tasks_history.json
	dbPath := flag.String("db-path", envDefault("DB_PATH", "/downloads/tasks_history.json"), "任务历史 JSON 持久化路径")
	flag.Parse()

	// 确保下载目录存在
	if err := os.MkdirAll(*downloadDir, 0755); err != nil {
		log.Fatalf("创建下载目录失败: %v", err)
	}

	log.Printf("=== DDM3U8 服务启动 ===")
	log.Printf("[启动] 配置: 端口=%s, 并发=%d, 下载目录=%s, 指纹=%s",
		*port, *maxParallel, *downloadDir, *fingerprint)
	log.Printf("[启动] 任务历史: %s", *dbPath)
	log.Printf("调度器已启动，最大并发下载数: %d", *maxParallel)

	srv := server.New(server.Config{
		Port:         *port,
		DownloadDir:  *downloadDir,
		TempBaseDir:  *downloadDir,
		FFmpegPath:   *ffmpegPath,
		Fingerprint:  *fingerprint,
		MaxParallel:  *maxParallel,
		WebUser:      *webUser,
		WebPass:      *webPass,
		DBPath:       *dbPath,
		TemplatesFS:  templatesFS,
	})
	log.Fatal(srv.ListenAndServe())
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
