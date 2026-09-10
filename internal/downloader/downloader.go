// Package downloader 提供 uTLS 指纹伪装的并发分片下载器。
package downloader

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"

	"github.com/pelico/DDM3U8-go/internal/m3u8"
	utls "github.com/refraction-networking/utls"
)

// Config 下载配置
type Config struct {
	URL          string
	Output       string // 合并后的最终输出文件路径
	TempDir      string // 临时分片目录
	Concurrency  int
	RetryMax     int
	RetryBackoff time.Duration
	Timeout      time.Duration
	Fingerprint  string // chrome / safari / firefox
	Referer      string
	UserAgent    string
	Headers      map[string]string
	SkipVerify   bool
	FFmpegPath   string // ffmpeg 二进制路径，空则跳过合并
	NoMerge      bool   // 跳过 ffmpeg 合并，仅输出分片

	// BestEffort=true 时：单个分片失败不立刻 cancel 所有 worker，让所有分片都跑完一次
	// 自己的 retry（更耗时间，但能拿到完整失败率统计）。
	// BestEffort=false（默认）时：保留旧的"一失败就全停"行为，避免长时间死磕。
	BestEffort bool

	// MaxFailureRate 失败率容忍阈值（0.0-1.0）。best-effort 模式下，所有分片都跑完后，
	// 如果 Failed/Total <= MaxFailureRate，下载器认为任务"可完成"（返回 nil err），
	// 上层进入合并阶段，把缺失分片跳过；ffmpeg 输出的 mp4 时长会变短但能播。
	// 设 0.05 = 允许 5% 分片失败；设 0 = 任何失败都视为失败。
	// 只在 BestEffort=true 时生效。
	MaxFailureRate float64
}

// Progress 进度快照，供 Web 端轮询
type Progress struct {
	Total      int    // 分片总数
	Done       int    // 已完成（成功）
	Failed     int    // 失败数
	Current    int    // 当前正在下载的分片序号（最近一个）
	Status     string // 阶段: fetching / downloading / merging / done / failed
	OutputFile string
	Note       string // 最近一条日志/错误信息
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		Concurrency:    3, // 默认 3，避免短时间大量握手触发 CDN bot 风控
		RetryMax:       10,
		RetryBackoff:   5 * time.Second, // 退避起步 5 秒，避免快速 retry 加剧风控
		Timeout:        60 * time.Second,
		Fingerprint:    "chrome",
		UserAgent:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
		FFmpegPath:     "ffmpeg",
		BestEffort:     true,           // 默认开：跑到所有分片都试过再决定
		MaxFailureRate: 0.05,           // 允许 5% 分片失败（CDN 偶发拒绝容错）
	}
}

// Result 下载结果
type Result struct {
	OutputFile string
	Segments   int
	Failed     int
	Duration   time.Duration
}

// Downloader 是核心下载器
type Downloader struct {
	cfg    Config
	httpc  *http.Client
	failed int32
	logger func(format string, v ...interface{})

	// 握手失败计数器：连续 uTLS handshake 失败次数，到阈值(5次)后
	// 通过 ctx cancel 自动暂停任务，避免无限触发 CDN bot 风控
	handshakeFails int32
	// OnHandshakeFails 由 TaskManager 注入的回调：握手失败到阈值时触发暂停
	OnHandshakeFails func()

	// 主动掐检测：单次重试轮里出现"连续 3 次 < 30s 快速失败"，判定 CDN 在主动掐流量
	// （区别于自然慢速 timeout——自然 timeout 恰好 60s 触发）。置位后让后续分片 timeout 翻倍。
	abuseFails int32
	// OnAbuseDetected 可选回调：检测到主动掐时由 TaskManager 注入（默认 nil，不暂停）。
	// 不再自动暂停——一个慢分片拖死整集太重，改为只延长 timeout。
	OnAbuseDetected func()

	// OnMergeStart 可选回调：进入 ffmpeg 合并阶段时触发，让上层把
	// task.Status 切到"合并中"（避免老镜像"卡在 100% 不变"的体验问题）。
	// 默认 nil，无副作用。
	OnMergeStart func()

	// 已下分片耗时跟踪：用于输出 seg 耗时分布日志（诊断主动掐/慢分片）
	segDurMu  sync.Mutex
	segDurs   []segDuration // 成功的分片耗时
	segFailed []segDuration // 失败的分片耗时 + 错误

	// 进度快照（原子读写，供 Web 端轮询）
	progress   Progress
	progressMu sync.RWMutex

	// 已下载字节数（含断点续传预扫描的已存在分片）
	// 用于估算最终文件大小：平均分片大小 × 总数
	downloadedBytes int64

	// 速度计算：滑动窗口记录最近完成的分片
	// 用最近若干分片的总大小/总时长算当前下载速度
	speedMu     sync.Mutex
	speedWindow []speedSample
}

type speedSample struct {
	at    time.Time
	bytes int64
}

// segDuration 单分片耗时样本（成功/失败共用）
type segDuration struct {
	segIndex int
	elapsed  time.Duration
	bytes    int64
	err      string // 失败时为错误信息，成功时为空
}

// 主动掐检测阈值
const (
	// 单次 HTTP 尝试 < 30s 算"快速失败"。自然慢 timeout 恰好走满 cfg.Timeout
	// （默认 60s），主动掐通常 < 30s 就 RST/断开。
	abuseFastFailThreshold = 30 * time.Second
	// 一次重试轮里"连续 N 次都快速失败"才告警。1-2 次可能是网络抖动，
	// 3 次连续基本就是被针对了。
	abuseConsecThreshold = int32(3)
	// 检测到主动掐后单分片 timeout 翻倍（60→120→240s），给 CDN 一个"等流量过去"的窗口。
	// 封顶 300s 防止把单分片拖到 5 分钟。
	abuseTimeoutCap = 300 * time.Second
)

// recordSpeed 记录一个分片完成事件，返回当前下载速度（bytes/s）
// 滑动窗口保留最近 30 秒的样本，过期自动清理
func (d *Downloader) recordSpeed(segBytes int64) float64 {
	now := time.Now()
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	d.speedWindow = append(d.speedWindow, speedSample{at: now, bytes: segBytes})
	// 清理 30 秒前的样本
	cutoff := now.Add(-30 * time.Second)
	idx := 0
	for ; idx < len(d.speedWindow); idx++ {
		if d.speedWindow[idx].at.After(cutoff) {
			break
		}
	}
	if idx > 0 {
		d.speedWindow = d.speedWindow[idx:]
	}
	// 算窗口内总字节 / 总时长
	if len(d.speedWindow) < 2 {
		return 0
	}
	var total int64
	for _, s := range d.speedWindow {
		total += s.bytes
	}
	dur := d.speedWindow[len(d.speedWindow)-1].at.Sub(d.speedWindow[0].at).Seconds()
	if dur <= 0 {
		return 0
	}
	return float64(total) / dur
}

// humanBytes 把字节数格式化为人类可读大小（如 2.8GB）
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// SegDurationSample 单分片耗时样本（导出给 TaskManager 输出耗时分布）
type SegDurationSample struct {
	SegIndex int
	Elapsed  time.Duration
	Bytes    int64
	Err      string // 失败时为错误信息，成功时为空
}

// SegDurations 返回成功和失败的 seg 耗时样本（深拷贝避免外部修改内部状态）
func (d *Downloader) SegDurations() (success, failed []SegDurationSample) {
	d.segDurMu.Lock()
	defer d.segDurMu.Unlock()
	for _, s := range d.segDurs {
		success = append(success, SegDurationSample{SegIndex: s.segIndex, Elapsed: s.elapsed, Bytes: s.bytes})
	}
	for _, s := range d.segFailed {
		failed = append(failed, SegDurationSample{SegIndex: s.segIndex, Elapsed: s.elapsed, Bytes: s.bytes, Err: s.err})
	}
	return
}

// New 创建下载器
func New(cfg Config, log func(format string, v ...interface{})) *Downloader {
	if log == nil {
		log = func(string, ...interface{}) {}
	}
	return &Downloader{cfg: cfg, logger: log}
}

// Progress 返回当前进度快照（goroutine-safe）
func (d *Downloader) Progress() Progress {
	d.progressMu.RLock()
	defer d.progressMu.RUnlock()
	return d.progress
}

// setProgress 原子更新进度
func (d *Downloader) setProgress(status, note string) {
	d.progressMu.Lock()
	defer d.progressMu.Unlock()
	d.progress.Status = status
	d.progress.Note = note
}

// Run 执行整个下载流程
func (d *Downloader) Run(ctx context.Context) (*Result, error) {
	start := time.Now()
	result := &Result{}
	d.setProgress("fetching", "拉取 m3u8")

	// 1. 构建带 uTLS 指纹的 HTTP 客户端
	if err := d.buildClient(); err != nil {
		d.setProgress("failed", fmt.Sprintf("build http client: %v", err))
		return nil, fmt.Errorf("build http client: %w", err)
	}

	// 2. 拉取并解析 m3u8
	playlist, err := d.fetchPlaylist(ctx, d.cfg.URL)
	if err != nil {
		d.setProgress("failed", fmt.Sprintf("fetch playlist: %v", err))
		return nil, fmt.Errorf("fetch playlist: %w", err)
	}

	// 3. 若是 master，选最高码率递归拉取
	for playlist.IsMaster && len(playlist.Variants) > 0 {
		best := playlist.Variants[0]
		for _, v := range playlist.Variants[1:] {
			if v.Bandwidth > best.Bandwidth {
				best = v
			}
		}
		d.logger("选中 variant: %d kbps (%s)", best.Bandwidth/1000, best.Resolution)
		playlist, err = d.fetchPlaylist(ctx, best.URI)
		if err != nil {
			return nil, fmt.Errorf("fetch variant playlist: %w", err)
		}
	}

	if len(playlist.Segments) == 0 {
		return nil, fmt.Errorf("no segments in playlist")
	}
	result.Segments = len(playlist.Segments)
	d.logger("分片总数: %d (live=%v)", result.Segments, playlist.IsLive)
	// key 轮换探测：解析阶段统计了不同 key URI 数量，>1 说明源用了轮换 key，
	// 当前实现只用最后一把 key 解全部分片，中间段会解错但不报错（花屏文件）。
	// 这里只打 warn 方便踩坑时定位，不改实际解密逻辑（暂不实现 key 轮换）。
	if playlist.KeyURICount > 1 {
		d.logger("[warn] 检测到 key 轮换: 共 %d 把不同 key URI，当前只用了最后一把 '%s'，前 %d 段可能被错解密（输出文件可能花屏）",
			playlist.KeyURICount, playlist.Encryption.URI, len(playlist.KeyURIsSample))
		for i, u := range playlist.KeyURIsSample {
			d.logger("[warn]   key #%d: %s", i+1, u)
		}
	}
	d.progressMu.Lock()
	d.progress.Total = result.Segments
	d.progress.Status = "downloading"
	// 断点续传预扫描：恢复任务时，进度从真实磁盘已存在分片数开始，
	// 避免前端看到"进度从 0 跳回真实值"的回退错觉
	d.progress.Done = 0
	d.progress.Failed = 0
	d.progress.Current = 0
	d.progressMu.Unlock()

	// 4. 准备临时目录
	if err := os.MkdirAll(d.cfg.TempDir, 0755); err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	// 4.5 预扫描已存在的完整分片文件（大小>0），预填 progress.Done
	// 这样恢复任务时前端进度条不会从 0 跳到真实值，避免"进度回退"错觉
	// 同时清理上次中断残留的 .part 占位文件，避免 downloadFile 的 O_EXCL 失败
	{
		existing := 0
		var existingBytes int64
		for _, seg := range playlist.Segments {
			p := filepath.Join(d.cfg.TempDir, fmt.Sprintf("seg_%05d.ts", seg.Index))
			if info, err := os.Stat(p); err == nil && info.Size() > 0 {
				existing++
				existingBytes += info.Size()
			} else {
				// 清理该分片对应的残留 .part 文件
				_ = os.Remove(p + ".part")
			}
		}
		if existing > 0 {
			atomic.StoreInt64(&d.downloadedBytes, existingBytes)
			d.progressMu.Lock()
			d.progress.Done = existing
			d.progressMu.Unlock()
			d.logger("断点续传: 已有 %d/%d 分片 (%s)，跳过这些", existing, result.Segments, humanBytes(existingBytes))
		}
	}

	// 5. 如有 EXT-X-MAP（fMP4 初始化段），先下载
	if playlist.MapURI != "" {
		mapPath := filepath.Join(d.cfg.TempDir, "_init.mp4")
		d.logger("下载 init segment: %s", playlist.MapURI)
		// init segment 通常不参与 AES-128 CBC 解密，传 nil key
		if err := d.downloadFile(ctx, playlist.MapURI, mapPath, playlist.MapBytes, nil, nil, 0); err != nil {
			return nil, fmt.Errorf("download init segment: %w", err)
		}
	}

	// 6. 获取解密 key（若有）
	var decryptKey []byte
	if playlist.Encryption != nil && playlist.Encryption.Method == "AES-128" {
		keyBytes, err := d.fetchKey(ctx, playlist.Encryption.URI)
		if err != nil {
			return nil, fmt.Errorf("fetch decryption key: %w", err)
		}
		decryptKey = keyBytes
		d.logger("获取到 AES-128 解密 key (%d bytes)", len(decryptKey))
	}

	// 7. 并发下载分片
	if err := d.downloadSegments(ctx, playlist, decryptKey); err != nil {
		return result, err
	}
	result.Failed = int(atomic.LoadInt32(&d.failed))

	// 8. 合并（ffmpeg concat）
	output := d.cfg.Output
	if d.cfg.NoMerge || d.cfg.FFmpegPath == "" {
		// 跳过合并，输出分片目录路径
		output = d.cfg.TempDir
		d.logger("跳过合并，分片位于: %s", output)
	} else {
		d.setProgress("merging", "ffmpeg 合并中")
		// 通知上层切到"合并中"状态：前端状态条会立刻从"下载中 100%"切到"合并中"
		if d.OnMergeStart != nil {
			d.OnMergeStart()
		}
		// 显式打 log 让前端的 t.Log 同步显示"合并中..."，否则会卡在最后一条
		// "下载中: X/X (100%)" 看上去像挂住，老镜像的已知问题。
		// 同时给前端一个估算的输出大小（用已下载字节推算），让进度条合理
		estBytes := atomic.LoadInt64(&d.downloadedBytes)
		d.logger("合并中: %s (%d 分片, 预估 %s)...",
			filepath.Base(output), result.Segments, humanBytes(estBytes))
		if err := d.merge(ctx, playlist, output); err != nil {
			d.setProgress("failed", fmt.Sprintf("merge: %v", err))
			return result, fmt.Errorf("merge: %w", err)
		}
	}
	result.OutputFile = output
	result.Duration = time.Since(start)
	d.logger("完成: %s (%d 分片, 失败 %d, 耗时 %s)",
		output, result.Segments, result.Failed, result.Duration.Truncate(time.Millisecond))
	d.progressMu.Lock()
	d.progress.OutputFile = output
	d.progress.Done = result.Segments - result.Failed
	d.progress.Failed = result.Failed
	d.progress.Status = "done"
	d.progressMu.Unlock()
	return result, nil
}

// buildClient 构建带 uTLS 指纹的 HTTP 客户端
func (d *Downloader) buildClient() error {
	tlsSpec := d.pickTLSSpec()
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	// 是否需要走 HTTP 代理（用于环境内的 egress）
	proxyStr := firstNonEmpty(os.Getenv("HTTPS_PROXY"), os.Getenv("https_proxy"),
		os.Getenv("HTTP_PROXY"), os.Getenv("http_proxy"))
	var proxyURL *url.URL
	if proxyStr != "" {
		proxyURL, _ = url.Parse(proxyStr)
	}

	// buildSpec 每次调用都新建一份 ClientHelloSpec。
	// ALPN 保留 uTLS Chrome 预设的 h2,http/1.1（不再强制锁死 h1）：
	// 真实 Chrome/手机 App 访问 Cloudflare 站点 2024 之后基本清一色 h2，
	// "ClientHello 说 h2 但实际走 h1" 本身就是干净的 bot 信号。
	// 服务器只支持 h1 时，http2.Transport 会自动回退用 h1 协议工作（库内置能力），
	// 不需要我们写回退分支。
	// spec 的扩展内部有可变状态（GREASE/KeyShare），多连接共享同一份 spec 会
	// 在并发握手时冲突 → "tls: internal error"。因此每连接独立构建 spec。
	buildSpec := func() (*utls.ClientHelloSpec, error) {
		spec, err := utls.UTLSIdToSpec(tlsSpec)
		if err != nil {
			return nil, err
		}
		return &spec, nil
	}

	dialTLS := func(network, addr string) (net.Conn, error) {
		var rawConn net.Conn
		var err error
		if proxyURL != nil && proxyURL.Scheme != "" {
			// 通过 HTTP CONNECT 代理建立到目标 host:port 的隧道
			rawConn, err = dialer.Dial(network, proxyURL.Host)
			if err != nil {
				return nil, fmt.Errorf("dial proxy %s: %w", proxyURL.Host, err)
			}
			connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", addr, addr)
			if _, err := rawConn.Write([]byte(connectReq)); err != nil {
				rawConn.Close()
				return nil, fmt.Errorf("write CONNECT: %w", err)
			}
			// 用 bufio 读响应行（不丢弃预读字节：之后 uTLS 接管 rawConn，
			// bufio 的缓冲区可能已预读多余数据。改用逐字节读取避免预读）
			br := bufio.NewReader(rawConn)
			line, err := br.ReadString('\n')
			if err != nil {
				rawConn.Close()
				return nil, fmt.Errorf("read CONNECT resp: %w", err)
			}
			if !strings.Contains(line, " 200 ") {
				rawConn.Close()
				return nil, fmt.Errorf("proxy CONNECT failed: %s", strings.TrimSpace(line))
			}
			// 丢弃剩余响应头直到空行
			for {
				l, err := br.ReadString('\n')
				if err != nil || strings.TrimSpace(l) == "" {
					break
				}
			}
			// 用 br（包含可能的预读字节）作为底层连接交给 uTLS
			rawConn = &bufferedReaderConn{Reader: br, Conn: rawConn}
		} else {
			rawConn, err = dialer.Dial(network, addr)
			if err != nil {
				return nil, err
			}
		}
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		spec, err := buildSpec() // 每连接独立 spec，避免并发握手冲突
		if err != nil {
			rawConn.Close()
			return nil, fmt.Errorf("build ClientHello spec: %w", err)
		}
		tlsConn := utls.UClient(rawConn, &utls.Config{
			ServerName:         host,
			InsecureSkipVerify: d.cfg.SkipVerify,
		}, utls.HelloCustom) // HelloCustom 留空 spec，靠 ApplyPreset 填
		if err := tlsConn.ApplyPreset(spec); err != nil {
			rawConn.Close()
			return nil, fmt.Errorf("apply ClientHello preset: %w", err)
		}
		if err := tlsConn.Handshake(); err != nil {
			rawConn.Close()
			// 握手失败计数：到阈值(5次)后触发自动暂停，避免无限触发 CDN bot 风控
			n := atomic.AddInt32(&d.handshakeFails, 1)
			d.logger("握手失败 %d/5: %v", n, err)
			if n >= 5 && d.OnHandshakeFails != nil {
				d.logger("连续握手失败 %d 次，自动暂停任务避免触发风控", n)
				go d.OnHandshakeFails()
			}
			return nil, fmt.Errorf("uTLS handshake: %w", err)
		}
		// 握手成功则重置计数
		atomic.StoreInt32(&d.handshakeFails, 0)
		// 输出实际协商的 ALPN 协议（h2/http1.1/空），用于和抓包对账
		alpn := tlsConn.ConnectionState().NegotiatedProtocol
		if alpn == "" {
			alpn = "(none/h1)"
		}
		d.logger("[h2探针] 握手成功 addr=%s ALPN=%s TLS=%s", addr, alpn, tlsConn.ConnectionState().Version)
		return tlsConn, nil
	}
	// h2 transport：客户端用 ALPN 协商 h2 时，多个分片请求会多路复用到同一条连接上，
	// 模拟真实浏览器/App 的"一条 h2 连接、多个 stream 并发取分片"行为；
	// 服务器只支持 h1 时，http2 库会自动回退 h1 协议工作。
	// DialTLSContext 是关键钩子：uTLS 在这里完成握手并返回 *utls.UConn，
	// UConn 实现了 net.Conn 接口，h2 transport 直接当 TCP+TLS 后的连接用。
	// dialTLS 闭包复用上面的代理 CONNECT + uTLS 握手逻辑。
	transport := &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			// 注：cfg.ServerName 已经被 Go 设置为 host（去除端口），直接用作 SNI
			conn, err := dialTLS(network, addr)
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
		// h2 库自己管理多路复用 + 内部 conn pool，不需要外部 MaxIdleConns* 字段
		IdleConnTimeout:    90 * time.Second,
		DisableCompression: true, // h2 自身支持 HPACK 头压缩，无需 transport 层做
		AllowHTTP:          false,
	}
	d.httpc = &http.Client{
		Transport: transport,
		Timeout:   d.cfg.Timeout,
	}
	return nil
}

// firstNonEmpty 返回第一个非空字符串
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// bufferedReaderConn 包装 net.Conn，让 Read 优先消费 bufio.Reader 的预读字节
type bufferedReaderConn struct {
	*bufio.Reader
	net.Conn
}

func (c *bufferedReaderConn) Read(b []byte) (int, error) {
	return c.Reader.Read(b)
}

// pickTLSSpec 按配置选择 ClientHello 指纹
// 默认 HelloChrome_133（uTLS v1.8.2 最高 Chrome 预设，与前端默认 UA Chrome/140 接近，
// 避免预设与 UA 声明的版本对不上被自洽性检查抓）
func (d *Downloader) pickTLSSpec() utls.ClientHelloID {
	switch strings.ToLower(d.cfg.Fingerprint) {
	case "safari":
		return utls.HelloSafari_16_0
	case "firefox":
		return utls.HelloFirefox_120
	case "chrome", "":
		return utls.HelloChrome_133
	default:
		return utls.HelloChrome_133
	}
}

// fetchPlaylist 下载 m3u8 文本
func (d *Downloader) fetchPlaylist(ctx context.Context, url string) (*m3u8.Playlist, error) {
	body, err := d.httpGet(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return m3u8.Parse(body, url)
}

// fetchKey 下载 AES key
func (d *Downloader) fetchKey(ctx context.Context, url string) ([]byte, error) {
	body, err := d.httpGet(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}

// httpGet 带自定义 header 的 GET
func (d *Downloader) httpGet(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	d.setHeaders(req)
	return d.doWithRetry(req)
}

// setHeaders 设置请求头
func (d *Downloader) setHeaders(req *http.Request) {
	if d.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", d.cfg.UserAgent)
	}
	if d.cfg.Referer != "" {
		req.Header.Set("Referer", d.cfg.Referer)
	}
	// 从 URL 推导 Origin
	if d.cfg.Referer != "" {
		if u, err := url.Parse(d.cfg.Referer); err == nil {
			req.Header.Set("Origin", fmt.Sprintf("%s://%s", u.Scheme, u.Host))
		}
	}
	// 浏览器标准头（真实浏览器访问视频时会发送这些）
	// 让请求头更自洽，降低被 CDN bot 检测识别的概率
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "*/*")
	}
	if req.Header.Get("Accept-Language") == "" {
		req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	}
	// ⚠️ Accept-Encoding 故意不显式设置：
	// 真实 Chrome 会发 "gzip, deflate, br, zstd"，但 Go http.Client 只自动解压 gzip，
	// 不会自动解压 br/zstd。若照抄会让 CDN 返回 brotli 压缩流，导致分片数据损坏。
	// 不设置时 Go Transport 会自动加 "gzip" 并自动解压，对二进制分片流最安全。
	if req.Header.Get("Sec-Fetch-Site") == "" {
		req.Header.Set("Sec-Fetch-Site", "cross-site")
	}
	if req.Header.Get("Sec-Fetch-Mode") == "" {
		req.Header.Set("Sec-Fetch-Mode", "cors")
	}
	if req.Header.Get("Sec-Fetch-Dest") == "" {
		req.Header.Set("Sec-Fetch-Dest", "empty")
	}
	// priority：HTTP/2 优先级提示，Chrome 124+ 会发，缺失易被识别为非浏览器
	if req.Header.Get("Priority") == "" {
		req.Header.Set("Priority", "u=1, i")
	}
	// sec-ch-ua 系列：根据 UA 自动推导，确保 UA 和 sec-ch-ua 自洽
	// （UA 是 Android 但 sec-ch-ua-platform 是 Windows 会被 CF 识别）
	ua := d.cfg.UserAgent
	if req.Header.Get("sec-ch-ua-mobile") == "" {
		if strings.Contains(ua, "Android") || strings.Contains(ua, "iPhone") || strings.Contains(ua, "Mobile") {
			req.Header.Set("sec-ch-ua-mobile", "?1")
		} else {
			req.Header.Set("sec-ch-ua-mobile", "?0")
		}
	}
	if req.Header.Get("sec-ch-ua-platform") == "" {
		switch {
		case strings.Contains(ua, "Android"):
			req.Header.Set("sec-ch-ua-platform", `"Android"`)
		case strings.Contains(ua, "iPhone"), strings.Contains(ua, "iPad"):
			req.Header.Set("sec-ch-ua-platform", `"iOS"`)
		case strings.Contains(ua, "Mac OS X"), strings.Contains(ua, "Macintosh"):
			req.Header.Set("sec-ch-ua-platform", `"macOS"`)
		case strings.Contains(ua, "Windows"), strings.Contains(ua, "Win64"):
			req.Header.Set("sec-ch-ua-platform", `"Windows"`)
		case strings.Contains(ua, "Linux"):
			req.Header.Set("sec-ch-ua-platform", `"Linux"`)
		default:
			req.Header.Set("sec-ch-ua-platform", `"Windows"`)
		}
	}
	if req.Header.Get("sec-ch-ua") == "" {
		// 从 UA 提取浏览器品牌和主版本号，构建 sec-ch-ua
		// brand 字符串随 Chrome 版本变化（120+ 常用 "Not_A Brand";v="8" / "Not)A;Brand" 等），
		// CF 不会精确匹配，格式自洽即可
		browser, version := parseBrowserFromUA(ua)
		req.Header.Set("sec-ch-ua", fmt.Sprintf(`"Not_A Brand";v="8", "%s";v="%s"`, browser, version))
	}
	// 用户自定义头会覆盖上面的默认值（放在最后赋值）
	for k, v := range d.cfg.Headers {
		req.Header.Set(k, v)
	}
}

// parseBrowserFromUA 从 UA 字符串提取浏览器品牌和主版本号，用于构建自洽的 sec-ch-ua
// UA 形如 "Mozilla/5.0 ... Chrome/140.0.0.0 ... Edg/140.0.0.0"
// 优先级：Edge > Chrome > Firefox > Safari（按 UA 后出现的品牌，浏览器会附在末尾）
func parseBrowserFromUA(ua string) (browser, version string) {
	uaLower := strings.ToLower(ua)
	// Edge 优先（Edge UA 末尾带 Edg/x，Chrome 在中间）
	if idx := strings.Index(uaLower, "edg/"); idx >= 0 {
		if v := extractVersion(ua[idx+4:]); v != "" {
			return "Microsoft Edge", v
		}
	}
	if idx := strings.Index(uaLower, "chrome/"); idx >= 0 {
		if v := extractVersion(ua[idx+7:]); v != "" {
			return "Google Chrome", v
		}
	}
	if idx := strings.Index(uaLower, "firefox/"); idx >= 0 {
		if v := extractVersion(ua[idx+8:]); v != "" {
			return "Firefox", v
		}
	}
	if idx := strings.Index(uaLower, "version/"); idx >= 0 {
		if v := extractVersion(ua[idx+8:]); v != "" {
			return "Safari", v
		}
	}
	return "Google Chrome", "126"
}

// extractVersion 从版本号字符串提取主版本（如 "140.0.0.0" → "140"）
func extractVersion(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			if i > 0 {
				return s[:i]
			}
			return ""
		}
	}
	return s
}

// doWithRetry 执行请求并按可重试错误退避重试
// 注意：退避等待用 select 监听 ctx.Done()，确保用户暂停/取消任务时
// 能立刻跳出 retry 循环，而不是继续把剩余次数重试完。
//
// 主动掐检测：单次尝试 < 30s 就报错视为"快速失败"（区别于自然慢 timeout），
// 一次重试轮里连续 3 次都快速失败 → 触发 OnAbuseDetected 回调。
func (d *Downloader) doWithRetry(req *http.Request) (io.ReadCloser, error) {
	var lastErr error
	backoff := d.cfg.RetryBackoff
	if backoff == 0 {
		backoff = time.Second
	}
	ctx := req.Context()
	var consecFastFails int32 // 本次重试轮里连续 < 30s 失败次数
	for i := 0; i < d.cfg.RetryMax; i++ {
		// 循环开头先检查 ctx，已被取消则立即退出，不再发起请求
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attemptStart := time.Now()
		resp, err := d.httpc.Do(req.Clone(ctx))
		attemptElapsed := time.Since(attemptStart)
		if err == nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				return resp.Body, nil
			}
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			// 4xx (除 429) 不重试。但也要计入"快速失败"——403/410 这种
			// 通常是 CF 主动拒绝，会快速返回
			if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 {
				if attemptElapsed < abuseFastFailThreshold {
					d.recordAbuseAttempt(&consecFastFails, attemptElapsed, lastErr)
				}
				return nil, lastErr
			}
		} else {
			lastErr = err
			// 请求失败若因 ctx 取消（用户暂停/取消），不再重试，也不算主动掐
			if ctx.Err() != nil {
				return nil, err
			}
			d.logger("retry %d/%d error: %v (耗时 %s)", i+1, d.cfg.RetryMax, err, attemptElapsed.Truncate(time.Millisecond))
			// 单次 < 30s 失败计一次"快速失败"，连续 3 次触发主动掐检测
			if attemptElapsed < abuseFastFailThreshold {
				d.recordAbuseAttempt(&consecFastFails, attemptElapsed, err)
			} else {
				// 走满 timeout 是慢速失败，不算主动掐，重置连续计数
				consecFastFails = 0
			}
		}
		// 退避等待，可被 ctx 取消打断（替代 time.Sleep）
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 60*time.Second {
			backoff = 60 * time.Second
		}
	}
	return nil, fmt.Errorf("after %d retries: %w", d.cfg.RetryMax, lastErr)
}

// recordAbuseAttempt 记录一次"快速失败"，连续达阈值时标记 abuseFails（让后续分片 timeout 翻倍）
// 注意：只标记，不自动暂停——一个慢分片拖死整集太重。任务继续跑，
// 后续分片 timeout 自动翻倍到 120s → 240s（封顶 300s），给 CDN 一个"等流量过去"的窗口。
// 真不行也会 retry 耗尽后正常失败，前端可手动暂停。
func (d *Downloader) recordAbuseAttempt(consec *int32, elapsed time.Duration, err error) {
	*consec++
	if *consec < abuseConsecThreshold {
		return
	}
	// 第一次到阈值才置位，避免重复触发
	if !atomic.CompareAndSwapInt32(&d.abuseFails, 0, 1) {
		return
	}
	d.logger("[warn] 检测到疑似主动掐流量: 连续 %d 次 < %v 失败 (例: %v)，后续分片 timeout 自动翻倍",
		*consec, abuseFastFailThreshold, err)
	// OnAbuseDetected 保留为可选钩子（默认 nil）：若 TaskManager 注入
	// 了回调可触发额外动作（如暂停）；未注入则只做 timeout 延长。
	if d.OnAbuseDetected != nil {
		d.OnAbuseDetected()
	}
}

// downloadSegments 并发下载所有分片
// 渐进爬升：第一片同步下，让 transport 暖起连接、避免"启动瞬间对同 host 突发 N 条新连接"
// 这种典型爬虫流量特征；之后才进 sem 限流的 goroutine 全量并发。
func (d *Downloader) downloadSegments(ctx context.Context, p *m3u8.Playlist, key []byte) error {
	sem := make(chan struct{}, d.cfg.Concurrency)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	// 派生 cancelCtx：第一个分片失败时立刻 cancel 所有 worker，
	// 避免 timeout 后还在等其它 goroutine 自然失败的几十秒/上百秒。
	// 这样 wg.Wait() 几乎立即返回，Run 也能快速把 firstErr 报给 task 层。
	cancelCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	// 预解析 IV：EXT-X-KEY IV 为 hex 时直接用，否则用分片序号兜底
	var fixedIV []byte
	if p.Encryption != nil && p.Encryption.IV != "" {
		if iv, err := parseHexIV(p.Encryption.IV); err == nil {
			fixedIV = iv
		}
	}

	// downloadOne 封装单分片下载逻辑（断点续传检测 + 成功/失败分支），
	// 启动阶段同步调用，剩余分片在 goroutine 内调用
	downloadOne := func(seg *m3u8.Segment) {
		outPath := filepath.Join(d.cfg.TempDir, fmt.Sprintf("seg_%05d.ts", seg.Index))
		// 校验文件存在且大小>0：避免跳过上次网络中断写一半的损坏分片
		if info, err := os.Stat(outPath); err == nil && info.Size() > 0 {
			// 已下载（断点续传），跳过
			// 注意：预扫描已把 Done 算进去了，这里只更新 Current，不重复 Done++
			d.progressMu.Lock()
			d.progress.Current = seg.Index
			d.progressMu.Unlock()
			return
		}
		if err := d.downloadFile(cancelCtx, seg.URI, outPath, seg.ByteRange, key, fixedIV, seg.Index); err != nil {
			atomic.AddInt32(&d.failed, 1)
			errMu.Lock()
			if firstErr == nil {
				firstErr = fmt.Errorf("seg %d: %w", seg.Index, err)
				// 失败处理分两种模式：
				//  1) BestEffort=false：第一个分片失败时立即 cancel 所有 worker，让其它分片快速收手
				//  2) BestEffort=true：不 cancel，让所有分片都跑完自己的 retry，最后根据失败率决定
				if !d.cfg.BestEffort {
					cancelAll()
				}
			}
			errMu.Unlock()
			d.logger("seg %d 失败: %v", seg.Index, err)
			d.progressMu.Lock()
			d.progress.Failed++
			d.progress.Current = seg.Index
			d.progress.Note = fmt.Sprintf("seg %d 失败: %v", seg.Index, err)
			d.progressMu.Unlock()
		} else {
			// 累加已下载字节（用于估算最终文件大小）
			var segBytes int64
			if info, err := os.Stat(outPath); err == nil {
				segBytes = info.Size()
				atomic.AddInt64(&d.downloadedBytes, segBytes)
			}
			// 计算当前下载速度（滑动窗口）
			speed := d.recordSpeed(segBytes)
			// 先更新进度，再通过 logger 把快照传给上层（避免上层再 RLock progressMu 读）
			d.progressMu.Lock()
			d.progress.Done++
			d.progress.Current = seg.Index
			snap := d.progress
			d.progressMu.Unlock()
			// 用 "progress done/total/failed/bytes/speed" 格式，上层据此前缀做节流展示
			bytes := atomic.LoadInt64(&d.downloadedBytes)
			d.logger("progress %d/%d/%d/%d/%.0f", snap.Done, snap.Total, snap.Failed, bytes, speed)
		}
	}

	// 阶段 1：同步下第一个分片，让 transport 完成首次 TLS 握手并把连接进 idle pool。
	// 后续 goroutine 可立刻复用这条连接，避免"启动瞬间对同 host 突发 N 条新握手"的
	// 典型爬虫流量特征（CF bot management 会看这个）。
	// ctx 取消时（暂停/取消）直接返回
	if len(p.Segments) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		downloadOne(&p.Segments[0])
		// BestEffort=false 时：第一个分片失败 → 立即返回（其他 worker 已被 cancelAll 杀掉）
		// BestEffort=true 时：忽略早期失败，让所有分片都跑完，最后按失败率判断
		if firstErr != nil && !d.cfg.BestEffort {
			return firstErr
		}
	}

	// 阶段 2：剩余分片并发下载，受 sem 限流
	for i := 1; i < len(p.Segments); i++ {
		seg := &p.Segments[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			downloadOne(seg)
		}()
	}
	wg.Wait()
	// BestEffort 模式：所有分片都跑完后，根据失败率判断任务是否"可完成"
	if d.cfg.BestEffort && firstErr != nil {
		d.progressMu.RLock()
		done, failed, total := d.progress.Done, d.progress.Failed, d.progress.Total
		d.progressMu.RUnlock()
		if total > 0 && done+failed >= total {
			// 所有分片都已尝试过（要么成功要么彻底失败），按 MaxFailureRate 判定
			failureRate := float64(failed) / float64(total)
			if failureRate <= d.cfg.MaxFailureRate {
				d.logger("✅ best-effort: 失败率 %.1f%% (≤ %.1f%% 阈值)，接受并进入合并",
					failureRate*100, d.cfg.MaxFailureRate*100)
				return nil // 视为下载成功，进入合并（缺失分片由 writeConcatList 跳过）
			}
			d.logger("⚠️ best-effort: 失败率 %.1f%% 超过 %.1f%% 阈值，任务失败",
				failureRate*100, d.cfg.MaxFailureRate*100)
		}
	}
	return firstErr
}

// downloadFile 下载单个分片到文件
// IV 计算：fixedIV 非空则用 fixedIV；否则用 segIndex 作为 16 字节 IV 的低 8 字节（big-endian）。
func (d *Downloader) downloadFile(ctx context.Context, segURL, outPath string, br *m3u8.ByteRange, key, fixedIV []byte, segIndex int) error {
	segStart := time.Now()
	// 主动掐后单分片 timeout 翻倍（60→120→240s），封顶 300s
	// 触发条件：abuseFails 标志被置位（doWithRetry 检测到 3 次连续 < 30s 快速失败）
	// 翻倍后给 CDN 一个"等流量过去"的窗口，再下不慢的分片就有机会过
	segTimeout := d.cfg.Timeout
	if atomic.LoadInt32(&d.abuseFails) > 0 {
		segTimeout *= 2
		if segTimeout > abuseTimeoutCap {
			segTimeout = abuseTimeoutCap
		}
	}
	segCtx, segCancel := context.WithTimeout(ctx, segTimeout)
	defer segCancel()

	// 请求间隔随机化：0-300ms 随机抖动，避免固定间隔批量请求
	// 真实浏览器请求分片间隔不固定，CDN bot 检测会看请求时序模式
	if jitter := time.Duration(rand.Intn(300)) * time.Millisecond; jitter > 0 {
		select {
		case <-segCtx.Done():
			return segCtx.Err()
		case <-time.After(jitter):
		}
	}
	// 原子占位：用 .part 临时文件 + O_EXCL 创建，防止恢复任务时旧 goroutine
	// 还未完全退出导致同一分片被重复下载（race condition）
	partPath := outPath + ".part"
	pf, err := os.OpenFile(partPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		// .part 已存在：要么上次中断残留(大小可能不全)，要么有 goroutine 在下载
		// 直接返回错误让上层当失败处理，避免重复请求加剧封锁
		return fmt.Errorf("seg %d already being downloaded or stale .part exists: %w", segIndex, err)
	}
	pf.Close()
	defer os.Remove(partPath) // 无论成功失败都清理 .part（成功后 outPath 已写入）

	req, err := http.NewRequestWithContext(segCtx, "GET", segURL, nil)
	if err != nil {
		return err
	}
	d.setHeaders(req)
	if br != nil {
		if br.Offset > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", br.Offset, br.Offset+br.Length-1))
		} else {
			req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", br.Length-1))
		}
	}
	body, err := d.doWithRetry(req)
	if err != nil {
		d.recordSegDuration(segIndex, time.Since(segStart), 0, err)
		return err
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		d.recordSegDuration(segIndex, time.Since(segStart), 0, err)
		return err
	}
	if key != nil {
		iv := computeIV(fixedIV, segIndex)
		dec, err := aesDecrypt(data, key, iv)
		if err != nil {
			d.recordSegDuration(segIndex, time.Since(segStart), 0, err)
			return err
		}
		data = dec
	}
	// 先写 .part 再 rename 为最终文件，确保 outPath 要么不存在要么完整
	// 避免 WriteFile 中途被中断留下半截损坏文件被误判为已完成
	if err := os.WriteFile(partPath, data, 0644); err != nil {
		d.recordSegDuration(segIndex, time.Since(segStart), 0, err)
		return err
	}
	if err := os.Rename(partPath, outPath); err != nil {
		d.recordSegDuration(segIndex, time.Since(segStart), 0, err)
		return err
	}
	d.recordSegDuration(segIndex, time.Since(segStart), int64(len(data)), nil)
	return nil
}

// recordSegDuration 记录分片耗时（成功/失败均记录）
func (d *Downloader) recordSegDuration(segIndex int, elapsed time.Duration, bytes int64, err error) {
	sd := segDuration{segIndex: segIndex, elapsed: elapsed, bytes: bytes}
	if err != nil {
		sd.err = err.Error()
	}
	d.segDurMu.Lock()
	if err != nil {
		d.segFailed = append(d.segFailed, sd)
	} else {
		d.segDurs = append(d.segDurs, sd)
	}
	d.segDurMu.Unlock()
}

// computeIV 计算解密 IV
func computeIV(fixedIV []byte, segIndex int) []byte {
	if len(fixedIV) == 16 {
		return fixedIV
	}
	iv := make([]byte, 16)
	// 序号放低 8 字节 big-endian（HLS 规范默认行为）
	u := uint64(segIndex)
	for i := 0; i < 8; i++ {
		iv[15-i] = byte(u >> (8 * i))
	}
	return iv
}

// parseHexIV 解析 EXT-X-KEY 的 IV（0x... 十六进制）
func parseHexIV(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		s = s[2:]
	}
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd hex length")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi := hexVal(s[i*2])
		lo := hexVal(s[i*2+1])
		if hi < 0 || lo < 0 {
			return nil, fmt.Errorf("invalid hex")
		}
		out[i] = byte(hi<<4 | lo)
	}
	return out, nil
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// aesDecrypt AES-128-CBC 解密，自动去 PKCS7 padding
func aesDecrypt(data, key, iv []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	if len(data)%16 != 0 {
		// 非 16 字节对齐，可能未加密或截断，原样返回
		return data, nil
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	mode := cipher.NewCBCDecrypter(block, iv)
	dec := make([]byte, len(data))
	mode.CryptBlocks(dec, data)
	// 去 PKCS7 padding：校验末 n 个字节是否都等于 n，
	// 不合法说明 key/IV 错误或数据损坏，直接报错而非静默截断产生损坏输出
	if n := int(dec[len(dec)-1]); n > 0 && n <= 16 && n <= len(dec) {
		for i := len(dec) - n; i < len(dec); i++ {
			if int(dec[i]) != n {
				return nil, fmt.Errorf("invalid PKCS7 padding (expected %d at %d, got %d)", n, i, dec[i])
			}
		}
		dec = dec[:len(dec)-n]
	}
	return dec, nil
}

// merge 用 ffmpeg concat demuxer 合并分片
// best-effort 模式：缺失的分片用 ffmpeg 生成的黑场占位填充，保持总时长一致
func (d *Downloader) merge(ctx context.Context, p *m3u8.Playlist, output string) error {
	// 1. 先扫一遍：哪些分片缺失？最大缺失时长是多少？
	var missing, totalSegs int
	var maxMissingDur float64
	missingIdxs := []int{}
	for i := range p.Segments {
		segPath := filepath.Join(d.cfg.TempDir, fmt.Sprintf("seg_%05d.ts", p.Segments[i].Index))
		if info, err := os.Stat(segPath); err == nil && info.Size() > 0 {
			continue
		}
		missing++
		missingIdxs = append(missingIdxs, i)
		if p.Segments[i].Duration > maxMissingDur {
			maxMissingDur = p.Segments[i].Duration
		}
		totalSegs++
	}

	// 2. 如果有缺失分片，生成一个黑场占位 .ts（时长取 maxMissingDur + 1s 余量）
	//    concat demuxer 用 duration 指令把同一占位文件复用为多段，每段指定原始时长
	placeholderPath := ""
	if len(missingIdxs) > 0 {
		placeholderPath = filepath.Join(d.cfg.TempDir, "_placeholder.ts")
		// 占位时长：max + 1s 余量（避免 ffmpeg 报 "file too short"）
		dur := maxMissingDur + 1.0
		if dur < 1 {
			dur = 1
		}
		genCmd := exec.CommandContext(ctx, d.cfg.FFmpegPath,
			"-y",
			"-f", "lavfi", "-i", fmt.Sprintf("color=size=1280x720:rate=25:duration=%.3f:color=black", dur),
			"-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
			"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
			"-c:a", "aac", "-b:a", "64k",
			"-f", "mpegts",
			placeholderPath,
		)
		genCmd.Stdout = os.Stdout
		genCmd.Stderr = os.Stderr
		if err := genCmd.Run(); err != nil {
			return fmt.Errorf("生成黑场占位失败: %w", err)
		}
	}

	// 3. 生成 concat list
	listPath := filepath.Join(d.cfg.TempDir, "_concat.txt")
	var buf bytes.Buffer
	if p.MapURI != "" {
		buf.WriteString("file '_init.mp4'\n")
	}
	for i := range p.Segments {
		idx := i
		segPath := filepath.Join(d.cfg.TempDir, fmt.Sprintf("seg_%05d.ts", p.Segments[idx].Index))
		if info, err := os.Stat(segPath); err == nil && info.Size() > 0 {
			fmt.Fprintf(&buf, "file 'seg_%05d.ts'\n", p.Segments[idx].Index)
		} else {
			// 缺失：用占位 + duration 指令指定原始时长
			fmt.Fprintf(&buf, "file '_placeholder.ts'\nduration %.3f\n", p.Segments[idx].Duration)
		}
	}
	if err := os.WriteFile(listPath, buf.Bytes(), 0644); err != nil {
		return err
	}
	if missing > 0 {
		d.logger("⚠️ best-effort 合并: %d/%d 个分片用黑场占位替代，输出 mp4 时长保持一致",
			missing, totalSegs)
	}

	// 4. ffmpeg concat 合并
	// 加 -movflags +faststart：把 moov atom 移到文件头，支持边下边播
	// 用 -nostats + -progress pipe:1 替代默认 stderr 刷屏，改为自己解析精简进度
	cmd := exec.CommandContext(ctx, d.cfg.FFmpegPath,
		"-y",
		"-nostats",
		"-progress", "pipe:1",
		"-f", "concat",
		"-safe", "0",
		"-i", listPath,
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
		"-movflags", "+faststart",
		output,
	)
	cmd.Dir = d.cfg.TempDir
	cmd.Stderr = os.Stderr

	pr, pw := io.Pipe()
	cmd.Stdout = pw
	if err := cmd.Start(); err != nil {
		return err
	}
	// 总时长来自 playlist（分片时长之和），用于算百分比
	var totalDur float64
	for i := range p.Segments {
		totalDur += p.Segments[i].Duration
	}
	go d.concatMergeProgress(pr, totalDur)
	err := cmd.Wait()
	pw.Close()
	pr.Close()
	return err
}

// formatBytes 把字节数格式化成人类可读大小（kB/MB/GB）。
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// concatMergeProgress 解析 ffmpeg -progress pipe:1 输出（key=value 行），
// 节流调用 logger 输出精简合并进度：size / bitrate / speed，若有总时长则显示百分比。
// totalDur 是视频总时长（秒），<=0 表示未知（如强合场景），只显示绝对 time。
func (d *Downloader) concatMergeProgress(pipe io.Reader, totalDur float64) {
	scanner := bufio.NewScanner(pipe)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var outTimeUs int64
	var size int64
	var bitrate, speed string
	lastLog := time.Time{}

	for scanner.Scan() {
		line := scanner.Text()
		key, val, ok := cutEqual(line)
		if !ok {
			continue
		}
		switch key {
		case "out_time_us":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				outTimeUs = n
			}
		case "total_size":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				size = n
			}
		case "bitrate":
			bitrate = val
		case "speed":
			speed = val
		}
		// 节流：>=800ms 刷一次，避免刷屏；progress=end 强制输出最后一行
		if key != "progress" && time.Since(lastLog) < 800*time.Millisecond {
			continue
		}
		lastLog = time.Now()
		d.logger(mergeProgressLine(outTimeUs, totalDur, size, bitrate, speed))
	}
}

// mergeProgressLine 组装合并进度日志。
func mergeProgressLine(outTimeUs int64, totalDur float64, size int64, bitrate, speed string) string {
	cur := float64(outTimeUs) / 1e6
	now := fmtDuration(cur)
	var buf strings.Builder
	if totalDur > 0 {
		pct := 0.0
		if totalDur > 0 {
			pct = cur / totalDur * 100
			if pct > 100 {
				pct = 100
			}
		}
		buf.WriteString(fmt.Sprintf("合并中: %.1f%% (%s/%s)", pct, now, fmtDuration(totalDur)))
	} else {
		buf.WriteString(fmt.Sprintf("合并中: %s", now))
	}
	if size > 0 {
		buf.WriteString(fmt.Sprintf(" | size=%s", formatBytes(size)))
	}
	if bitrate != "" {
		buf.WriteString(fmt.Sprintf(" | bitrate=%s", bitrate))
	}
	if speed != "" {
		buf.WriteString(fmt.Sprintf(" | speed=%s", speed))
	}
	return buf.String()
}

// fmtDuration 把秒转成 hh:mm:ss。
func fmtDuration(sec float64) string {
	s := int(sec)
	h := s / 3600
	m := (s % 3600) / 60
	ss := s % 60
	if h > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, ss)
	}
	return fmt.Sprintf("%02d:%02d", m, ss)
}

// cutEqual 按第一个 '=' 拆分 key/value。
func cutEqual(s string) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

// MergeOnly 只执行合并步骤：扫描 tempDir 中的 .ts 分片文件（按文件名升序），
// 用 ffmpeg concat 合并成 output。用于"强合"按钮——复用已下载的分片，不重新下载。
// 如果存在 _init.mp4（fMP4 初始化段），会自动放在第一位。
func (d *Downloader) MergeOnly(ctx context.Context, tempDir, output string) error {
	// 扫描所有 .ts 分片，按文件名升序排序
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return fmt.Errorf("read temp dir: %w", err)
	}
	var segs []string
	var hasInit bool
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name == "_init.mp4" {
			hasInit = true
			continue
		}
		if filepath.Ext(name) == ".ts" {
			segs = append(segs, name)
		}
	}
	if len(segs) == 0 {
		return fmt.Errorf("temp dir %s 下没有 .ts 分片文件", tempDir)
	}
	// 按文件名升序（seg_00000.ts, seg_00001.ts, ... 自然顺序）
	sort.Strings(segs)

	listPath := filepath.Join(tempDir, "_concat.txt")
	var buf bytes.Buffer
	if hasInit {
		buf.WriteString("file '_init.mp4'\n")
	}
	for _, name := range segs {
		fmt.Fprintf(&buf, "file '%s'\n", name)
	}
	if err := os.WriteFile(listPath, buf.Bytes(), 0644); err != nil {
		return err
	}

	d.logger("强合: %d 个分片%s -> %s", len(segs),
		map[bool]string{true: " (含 init)", false: ""}[hasInit], output)
	d.logger("正在执行 ffmpeg concat 合并...")

	cmd := exec.CommandContext(ctx, d.cfg.FFmpegPath,
		"-y",
		"-nostats",
		"-progress", "pipe:1",
		"-f", "concat",
		"-safe", "0",
		"-i", listPath,
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
		"-movflags", "+faststart",
		output,
	)
	cmd.Dir = tempDir
	// 捕获 ffmpeg stderr 到 buffer，合并后输出到 logger（避免 io.Pipe 无缓冲
	// 导致 ffmpeg 写阻塞 + logger 回调抢 m.mu.Lock 时链式阻塞）
	var stderrBuf bytes.Buffer
	cmd.Stderr = io.MultiWriter(&stderrBuf, os.Stderr)

	// 进度走 -progress pipe:1；强合没有 playlist，总时长未知，只显示 size/bitrate/speed
	pr, pw := io.Pipe()
	cmd.Stdout = pw
	if err := cmd.Start(); err != nil {
		return err
	}
	go d.concatMergeProgress(pr, 0)
	err = cmd.Wait()
	pw.Close()
	pr.Close()
	// 合并完成后输出 ffmpeg 日志（最后 20 行，避免过长）
	lines := strings.Split(strings.TrimSpace(stderrBuf.String()), "\n")
	start := 0
	if len(lines) > 20 {
		start = len(lines) - 20
	}
	for _, line := range lines[start:] {
		if line != "" {
			d.logger("ffmpeg: %s", line)
		}
	}
	return err
}
