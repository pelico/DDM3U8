// ddm3u8-go 是一个用 Go + uTLS 写的 m3u8 下载器，
// 支持浏览器 TLS 指纹伪装、并发分片下载、AES-128 解密、ffmpeg 合并。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pelico/DDM3U8-go/internal/downloader"
)

func main() {
	cfg := downloader.DefaultConfig()

	var (
		url         = flag.String("url", "", "m3u8 URL (必填)")
		saveDir     = flag.String("save-dir", ".", "保存目录")
		tempDir     = flag.String("temp-dir", "", "临时分片目录（默认 saveDir/<name>_temp）")
		concurrency = flag.Int("concurrency", 10, "并发分片数")
		retries     = flag.Int("retries", 10, "单分片最大重试次数")
		timeout     = flag.Int("timeout", 60, "单分片超时（秒）")
		fingerprint = flag.String("fingerprint", "chrome", "TLS 指纹: chrome/safari/firefox")
		referer     = flag.String("referer", "", "Referer header")
		ua          = flag.String("user-agent", "", "User-Agent header（空则用默认 Chrome）")
		skipVerify  = flag.Bool("insecure", false, "跳过 TLS 证书校验")
		ffmpegPath  = flag.String("ffmpeg", "ffmpeg", "ffmpeg 二进制路径，留空跳过合并")
		saveName    = flag.String("save-name", "", "保存文件名（不含扩展名）")
		noMerge     = flag.Bool("no-merge", false, "跳过 ffmpeg 合并，仅保留分片")
		verbose     = flag.Bool("v", false, "输出详细日志（含每分片进度）")
	)
	flag.Parse()

	if *url == "" {
		fmt.Fprintln(os.Stderr, "用法: ddm3u8-go -url <m3u8-url> [选项]")
		flag.Usage()
		os.Exit(1)
	}

	cfg.URL = *url
	cfg.Concurrency = *concurrency
	cfg.RetryMax = *retries
	cfg.Timeout = time.Duration(*timeout) * time.Second
	cfg.Fingerprint = *fingerprint
	cfg.Referer = *referer
	cfg.SkipVerify = *skipVerify
	cfg.FFmpegPath = *ffmpegPath
	cfg.NoMerge = *noMerge
	if *ua != "" {
		cfg.UserAgent = *ua
	}
	cfg.Headers = map[string]string{}

	// 计算输出路径
	name := *saveName
	if name == "" {
		// 用时间戳生成默认名
		name = fmt.Sprintf("video_%s", time.Now().Format("0102_150405"))
	}
	if err := os.MkdirAll(*saveDir, 0755); err != nil {
		log.Fatalf("创建 save-dir 失败: %v", err)
	}
	cfg.Output = filepath.Join(*saveDir, name+".mp4")
	if *tempDir == "" {
		cfg.TempDir = filepath.Join(*saveDir, name+"_temp")
	} else {
		cfg.TempDir = *tempDir
	}

	// 处理信号（Ctrl+C）
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Printf("ddm3u8-go 启动: %s", *url)
	log.Printf("输出: %s | temp: %s | 并发: %d | 指纹: %s",
		cfg.Output, cfg.TempDir, cfg.Concurrency, cfg.Fingerprint)
	if cfg.Referer != "" {
		log.Printf("Referer: %s", cfg.Referer)
	}

	d := downloader.New(cfg, func(format string, v ...interface{}) {
		if !*verbose {
			// 非 verbose 时只打印关键日志（启动/完成/失败汇总/重试/错误）
			msg := fmt.Sprintf(format, v...)
			switch {
			case strings.HasPrefix(msg, "seg ") && strings.HasSuffix(msg, " ok"):
				return
			}
		}
		log.Printf(format, v...)
	})
	result, err := d.Run(ctx)
	if err != nil {
		log.Printf("❌ 失败: %v", err)
		if result != nil {
			log.Printf("  分片: %d, 失败: %d", result.Segments, result.Failed)
		}
		os.Exit(1)
	}
	log.Printf("✅ 完成: %s (%d 分片, 失败 %d, 耗时 %s)",
		result.OutputFile, result.Segments, result.Failed, result.Duration.Truncate(time.Millisecond))
}
