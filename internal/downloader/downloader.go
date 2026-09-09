// Package downloader 提供 uTLS 指纹伪装的并发分片下载器。
package downloader

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
}

// Progress 进度快照，供 Web 端轮询
type Progress struct {
	Total     int   // 分片总数
	Done      int   // 已完成（成功）
	Failed    int   // 失败数
	Current   int   // 当前正在下载的分片序号（最近一个）
	Status    string // 阶段: fetching / downloading / merging / done / failed
	OutputFile string
	Note      string // 最近一条日志/错误信息
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		Concurrency:  3, // 默认 3，避免短时间大量握手触发 CDN bot 风控
		RetryMax:     10,
		RetryBackoff: 5 * time.Second, // 退避起步 5 秒，避免快速 retry 加剧风控
		Timeout:      60 * time.Second,
		Fingerprint:  "chrome",
		UserAgent:    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
		FFmpegPath:   "ffmpeg",
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

	// 进度快照（原子读写，供 Web 端轮询）
	progress     Progress
	progressMu   sync.RWMutex
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
	d.progressMu.Lock()
	d.progress.Total = result.Segments
	d.progress.Status = "downloading"
	d.progressMu.Unlock()

	// 4. 准备临时目录
	if err := os.MkdirAll(d.cfg.TempDir, 0755); err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
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

	// buildSpec 每次调用都新建一份 ClientHelloSpec，并把 ALPN 改为仅 http/1.1。
	// 原因：uTLS Chrome 预设的 ALPN(h2,http/1.1) 会覆盖 Config.NextProtos，
	// 服务器协商到 h2 后，标准 http.Transport（只会说 h1）会因收到 h2 SETTINGS
	// 帧报 "malformed HTTP response" → EOF。
	// 且 spec 的扩展内部有可变状态（GREASE/KeyShare），多连接共享同一份 spec 会
	// 在并发握手时冲突 → "tls: internal error"。因此每连接独立构建 spec。
	buildSpec := func() (*utls.ClientHelloSpec, error) {
		spec, err := utls.UTLSIdToSpec(tlsSpec)
		if err != nil {
			return nil, err
		}
		for _, ext := range spec.Extensions {
			if alpn, ok := ext.(*utls.ALPNExtension); ok {
				alpn.AlpnProtocols = []string{"http/1.1"}
			}
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
		return tlsConn, nil
	}
	transport := &http.Transport{
		DialTLS:             dialTLS,
		MaxIdleConns:        d.cfg.Concurrency * 2,
		MaxIdleConnsPerHost: d.cfg.Concurrency * 2, // 默认只有 2，10 并发分片同 host 会导致 8 个请求无法复用连接→每分片都新建 TCP+uTLS 握手，CPU 飙升
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
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
func (d *Downloader) pickTLSSpec() utls.ClientHelloID {
	switch strings.ToLower(d.cfg.Fingerprint) {
	case "safari":
		return utls.HelloSafari_16_0
	case "firefox":
		return utls.HelloFirefox_105
	case "chrome", "":
		return utls.HelloChrome_106_Shuffle
	default:
		return utls.HelloChrome_106_Shuffle
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
	for k, v := range d.cfg.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
}

// doWithRetry 执行请求并按可重试错误退避重试
// 注意：退避等待用 select 监听 ctx.Done()，确保用户暂停/取消任务时
// 能立刻跳出 retry 循环，而不是继续把剩余次数重试完。
func (d *Downloader) doWithRetry(req *http.Request) (io.ReadCloser, error) {
	var lastErr error
	backoff := d.cfg.RetryBackoff
	if backoff == 0 {
		backoff = time.Second
	}
	ctx := req.Context()
	for i := 0; i < d.cfg.RetryMax; i++ {
		// 循环开头先检查 ctx，已被取消则立即退出，不再发起请求
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, err := d.httpc.Do(req.Clone(ctx))
		if err == nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				return resp.Body, nil
			}
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 {
				return nil, lastErr // 4xx 不重试（429 除外）
			}
		} else {
			lastErr = err
			// 请求失败若因 ctx 取消（用户暂停/取消），不再重试
			if ctx.Err() != nil {
				return nil, err
			}
			d.logger("retry %d/%d error: %v", i+1, d.cfg.RetryMax, err)
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

// downloadSegments 并发下载所有分片
func (d *Downloader) downloadSegments(ctx context.Context, p *m3u8.Playlist, key []byte) error {
	sem := make(chan struct{}, d.cfg.Concurrency)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	// 预解析 IV：EXT-X-KEY IV 为 hex 时直接用，否则用分片序号兜底
	var fixedIV []byte
	if p.Encryption != nil && p.Encryption.IV != "" {
		if iv, err := parseHexIV(p.Encryption.IV); err == nil {
			fixedIV = iv
		}
	}

	for i := range p.Segments {
		seg := p.Segments[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			outPath := filepath.Join(d.cfg.TempDir, fmt.Sprintf("seg_%05d.ts", seg.Index))
			if _, err := os.Stat(outPath); err == nil {
				// 已下载（断点续传），跳过
				d.progressMu.Lock()
				d.progress.Done++
				d.progress.Current = seg.Index
				d.progressMu.Unlock()
				return
			}
			if err := d.downloadFile(ctx, seg.URI, outPath, seg.ByteRange, key, fixedIV, seg.Index); err != nil {
				atomic.AddInt32(&d.failed, 1)
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("seg %d: %w", seg.Index, err)
				}
				errMu.Unlock()
				d.logger("seg %d 失败: %v", seg.Index, err)
				d.progressMu.Lock()
				d.progress.Failed++
				d.progress.Current = seg.Index
				d.progress.Note = fmt.Sprintf("seg %d 失败: %v", seg.Index, err)
				d.progressMu.Unlock()
			} else {
				// 先更新进度，再通过 logger 把快照传给上层（避免上层再 RLock progressMu 读）
				d.progressMu.Lock()
				d.progress.Done++
				d.progress.Current = seg.Index
				snap := d.progress
				d.progressMu.Unlock()
				// 用 "progress done/total/failed" 格式，上层据此前缀做节流展示
				d.logger("progress %d/%d/%d", snap.Done, snap.Total, snap.Failed)
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// downloadFile 下载单个分片到文件
// IV 计算：fixedIV 非空则用 fixedIV；否则用 segIndex 作为 16 字节 IV 的低 8 字节（big-endian）。
func (d *Downloader) downloadFile(ctx context.Context, segURL, outPath string, br *m3u8.ByteRange, key, fixedIV []byte, segIndex int) error {
	req, err := http.NewRequestWithContext(ctx, "GET", segURL, nil)
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
		return err
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if key != nil {
		iv := computeIV(fixedIV, segIndex)
		dec, err := aesDecrypt(data, key, iv)
		if err != nil {
			return err
		}
		data = dec
	}
	return os.WriteFile(outPath, data, 0644)
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
	// 去 PKCS7 padding
	if n := int(dec[len(dec)-1]); n > 0 && n <= 16 {
		dec = dec[:len(dec)-n]
	}
	return dec, nil
}

// merge 用 ffmpeg concat demuxer 合并分片
func (d *Downloader) merge(ctx context.Context, p *m3u8.Playlist, output string) error {
	// 生成 concat list 文件
	listPath := filepath.Join(d.cfg.TempDir, "_concat.txt")
	var buf bytes.Buffer
	if p.MapURI != "" {
		buf.WriteString("file '_init.mp4'\n")
	}
	for i := range p.Segments {
		fmt.Fprintf(&buf, "file 'seg_%05d.ts'\n", p.Segments[i].Index)
	}
	if err := os.WriteFile(listPath, buf.Bytes(), 0644); err != nil {
		return err
	}

	// ffmpeg -f concat -safe 0 -i list -c copy out.mp4
	cmd := exec.CommandContext(ctx, d.cfg.FFmpegPath,
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listPath,
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
		output,
	)
	cmd.Dir = d.cfg.TempDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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
		"-f", "concat",
		"-safe", "0",
		"-i", listPath,
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
		output,
	)
	cmd.Dir = tempDir
	// 捕获 ffmpeg stderr 到 buffer，合并后输出到 logger（避免 io.Pipe 无缓冲
	// 导致 ffmpeg 写阻塞 + logger 回调抢 m.mu.Lock 时链式阻塞）
	var stderrBuf bytes.Buffer
	cmd.Stdout = nil
	cmd.Stderr = io.MultiWriter(&stderrBuf, os.Stderr)
	err = cmd.Run()
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
