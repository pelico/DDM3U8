// Package server 提供 Web 服务，API 兼容原 Flask 版。
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/pelico/DDM3U8-go/internal/downloader"
)

// 任务状态常量（与原 Flask 前端展示对齐）
const (
	StatusQueued     = "排队中"
	StatusDownload   = "下载中"
	StatusMerge      = "合并中"
	StatusConverting = "转换中" // 音频提取（ffmpeg 转 .m4a）
	StatusDone       = "已完成"
	StatusFailed     = "失败"
	StatusCanceled   = "已取消"
	StatusPaused     = "已暂停"
)

// Task 单个下载任务
type Task struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	URL        string    `json:"url"`
	Status     string    `json:"status"`
	Log        string    `json:"log"`
	Cmd        string    `json:"cmd,omitempty"`
	OutputFile string    `json:"output_file,omitempty"`
	CreatedAt  time.Time `json:"created_at"`

	// 内部引用
	cfg       downloader.Config
	cancel    context.CancelFunc
	dl        *downloader.Downloader
	startedAt time.Time
	cmd       string

	// 对外可见字段（与 armv7l 分支 Python 版对齐，中间件依赖这些字段同步任务）
	DownloadDir  string            `json:"download_dir,omitempty"`
	TempDir      string            `json:"temp_dir,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	FolderTarget string            `json:"folder_target,omitempty"`
	AudioTarget  *AudioTarget      `json:"audio_target,omitempty"`

	// CDN 预热结果（任务开始前对 m3u8 主机做 HEAD 测试）
	// PrewarmStatus: "unknown"(未测) / "ok"(200) / "warn"(>=400但<500) / "fail"(网络错误或5xx)
	// PrewarmMs: HEAD 请求耗时（毫秒，<=0 表示测速失败）
	PrewarmStatus string `json:"prewarm_status,omitempty"`
	PrewarmMs     int64  `json:"prewarm_ms,omitempty"`
	PrewarmHost   string `json:"prewarm_host,omitempty"`
}

// AudioTarget 音频提取任务的目标文件对（与 Python 版 audio_target 同形）
type AudioTarget struct {
	InputPath  string `json:"input_path"`
	OutputPath string `json:"output_path"`
}

// TaskManager 管理所有任务
type TaskManager struct {
	mu          sync.RWMutex
	tasks       map[string]*Task
	order       []string // 按创建时间倒序
	maxParallel int

	downloadDir string
	tempBaseDir string
	ffmpegPath  string
	fingerprint string
	dbPath      string // 任务历史持久化路径（空则不持久化）

	// saveMu 串行化 saveTasks 调用：多个协程可能并发调 saveTasks
	//（如 runTask 完成 + 前端轮询触发 Snapshot + Delete 批量操作），
	// 不加锁会并发 json.Marshal + WriteFile 浪费 CPU 和 IO
	saveMu sync.Mutex

	// 列表缓存：ListVideoFiles / ListFolders 每次 API 调用都全盘 Walk
	// 几十 GB 视频库时前端明显卡。加 5s TTL 自动过期（不手动 invalidate：
	// 新建/完成任务让前端能晚 5s 看到结果，代价小；避免增加 API 路径耦合）
	listCacheMu     sync.Mutex
	listCacheAt     time.Time
	listCacheVideos []map[string]interface{}
	listCacheFolds  []string

	// 全局下载速度统计：聚合所有活跃 downloader 的瞬时速度。
	// 设计要点：
	//   - 每个 downloader 自己维持 30s 滑动窗口算自己的瞬时速度
	//   - TaskManager 维持一个全局滑动窗口，按"段结束时间"插样本，每次有分片完成
	//     就 push 一条 (at=now, bytes=segBytes) 到 samples
	//   - 当前总速度 = samples 中 30s 窗口内总字节 / 30s
	//   - 峰值速度 = 历史所有样本中算出的最大值，启动后单调不减
	// 用 mutex（不常用，锁粒度只覆盖 30s 窗口操作）即可，没必要上 atomic。
	speedMu       sync.Mutex
	speedSamples  []speedSample
	speedPeak     float64 // B/s，启动后单调不减
	speedPeakAt   time.Time
	speedStartMon time.Time // 首次有样本的时间（用于过滤启动期的瞬时尖峰）
}

// speedSample 全局速度窗口的一个样本：某时刻完成了一段，共若干字节
type speedSample struct {
	at    time.Time
	bytes int64
}

// 速度窗口宽度：最近 30 秒累计字节 / 30 秒 = 当前 B/s
const speedWindow = 30 * time.Second

// recordGlobalSpeed 由 TaskManager 在 downloader 上报分片完成时调用。
// 把 segBytes 在 now 时累入窗口，30s 之外的样本丢弃，
// 同时按窗口算一次"当前总速度"，超过历史峰值则更新峰值。
func (m *TaskManager) recordGlobalSpeed(segBytes int64) {
	if segBytes <= 0 {
		return
	}
	m.speedMu.Lock()
	defer m.speedMu.Unlock()
	now := time.Now()
	m.speedSamples = append(m.speedSamples, speedSample{at: now, bytes: segBytes})
	cutoff := now.Add(-speedWindow)
	idx := 0
	for ; idx < len(m.speedSamples); idx++ {
		if m.speedSamples[idx].at.After(cutoff) {
			break
		}
	}
	if idx > 0 {
		m.speedSamples = m.speedSamples[idx:]
	}
	// 启动后 1 秒内不更新峰值（避免初始一两个分片瞬间出"虚假峰值"）
	if m.speedStartMon.IsZero() {
		m.speedStartMon = now
	}
	if now.Sub(m.speedStartMon) < time.Second {
		return
	}
	if len(m.speedSamples) < 2 {
		return
	}
	var total int64
	for _, s := range m.speedSamples {
		total += s.bytes
	}
	dur := m.speedSamples[len(m.speedSamples)-1].at.Sub(m.speedSamples[0].at).Seconds()
	if dur <= 0 {
		return
	}
	cur := float64(total) / dur
	if cur > m.speedPeak {
		m.speedPeak = cur
		m.speedPeakAt = now
	}
}

// StatsSnapshot 暴露给前端的全局状态快照（/api/stats 用）
type StatsSnapshot struct {
	ActiveWorkers int     `json:"active_workers"`  // 当前正在下载/合并/转换的任务数
	MaxWorkers    int     `json:"max_workers"`     // 并发上限
	QueuedTasks   int     `json:"queued_tasks"`    // 排队中的任务数
	CurrentSpeed  float64 `json:"current_speed"`   // 当前总下载速度 B/s
	PeakSpeed     float64 `json:"peak_speed"`      // 历史峰值 B/s
	PeakAt        string  `json:"peak_at,omitempty"` // 峰值发生时间（RFC3339，空=未发生）
}

// Stats 返回全局状态快照（goroutine-safe）
func (m *TaskManager) Stats() StatsSnapshot {
	m.mu.RLock()
	active, queued := 0, 0
	for _, id := range m.order {
		t := m.tasks[id]
		switch t.Status {
		case StatusDownload, StatusMerge:
			active++
		case StatusQueued:
			queued++
		}
	}
	m.mu.RUnlock()

	m.speedMu.Lock()
	now := time.Now()
	cutoff := now.Add(-speedWindow)
	// 清理过期样本
	idx := 0
	for ; idx < len(m.speedSamples); idx++ {
		if m.speedSamples[idx].at.After(cutoff) {
			break
		}
	}
	if idx > 0 {
		m.speedSamples = m.speedSamples[idx:]
	}
	var currentSpeed float64
	if len(m.speedSamples) >= 2 {
		var total int64
		for _, s := range m.speedSamples {
			total += s.bytes
		}
		dur := m.speedSamples[len(m.speedSamples)-1].at.Sub(m.speedSamples[0].at).Seconds()
		if dur > 0 {
			currentSpeed = float64(total) / dur
		}
	}
	peak := m.speedPeak
	var peakAt string
	if !m.speedPeakAt.IsZero() {
		peakAt = m.speedPeakAt.UTC().Format(time.RFC3339)
	}
	m.speedMu.Unlock()

	return StatsSnapshot{
		ActiveWorkers: active,
		MaxWorkers:    m.maxParallel,
		QueuedTasks:   queued,
		CurrentSpeed:  currentSpeed,
		PeakSpeed:     peak,
		PeakAt:        peakAt,
	}
}

// listCacheTTL 列表缓存有效期。短到能让"新文件"尽快可见，
// 长到能把高频轮询的 Walk 全部合并掉。
const listCacheTTL = 5 * time.Second

// NewTaskManager 创建任务管理器
func NewTaskManager(maxParallel int, downloadDir, tempBaseDir, ffmpegPath, fingerprint, dbPath string) *TaskManager {
	if maxParallel < 1 {
		maxParallel = 1
	}
	return &TaskManager{
		tasks:       map[string]*Task{},
		maxParallel: maxParallel,
		downloadDir: downloadDir,
		tempBaseDir: tempBaseDir,
		ffmpegPath:  ffmpegPath,
		fingerprint: fingerprint,
		dbPath:      dbPath,
	}
}

// Create 创建新任务（使用默认下载目录）
func (m *TaskManager) Create(url, name string, headers map[string]string) string {
	return m.CreateWithDir(url, name, headers, m.downloadDir, 0, "")
}

// CreateWithDir 创建新任务（指定下载目录，用于 sub_path）
// concurrency/fingerprint 由前端传入，未传则用 manager 默认值（默认3并发、chrome指纹）
func (m *TaskManager) CreateWithDir(url, name string, headers map[string]string, downloadDir string, concurrency int, fingerprint string) string {
	id := uuid.New().String()[:8]
	ts := time.Now().Format("0102_150405")
	fullName := fmt.Sprintf("%s_%s_%s", name, ts, id[:3])

	cfg := downloader.DefaultConfig()
	cfg.URL = url
	cfg.FFmpegPath = m.ffmpegPath
	cfg.Fingerprint = m.fingerprint
	if fingerprint != "" {
		cfg.Fingerprint = fingerprint // 前端传入的指纹覆盖全局
	}
	if concurrency > 0 {
		cfg.Concurrency = concurrency // 前端传入的并发覆盖默认
	}
	cfg.Headers = headers
	cfg.Output = filepath.Join(downloadDir, fullName+".mp4")
	cfg.TempDir = filepath.Join(downloadDir, fullName+"_temp")
	if ua := headers["User-Agent"]; ua != "" {
		cfg.UserAgent = ua
	}
	if ref := headers["Referer"]; ref != "" {
		cfg.Referer = ref
	}

	task := &Task{
		ID:        id,
		Name:      fullName,
		URL:       url,
		Status:    StatusQueued,
		Log:       "等待执行...",
		CreatedAt: time.Now(),
		cfg:       cfg,
		// 对外可见字段：中间件通过 download_dir / temp_dir / headers 同步任务进度
		DownloadDir: downloadDir,
		TempDir:     cfg.TempDir,
		Headers:     headers,
	}
	task.cmd = buildCmdString(cfg)

	m.mu.Lock()
	m.tasks[id] = task
	m.order = append([]string{id}, m.order...)
	m.mu.Unlock()

	// 异步做 CDN 预热（HEAD 测试 m3u8 主机响应时间和状态码）：
	//   - 给前端一个"CDN 状态"图标显示，避免"先看到失败才知道 CDN 不通"
	//   - 用独立 goroutine + 短超时（3s），不影响任务正常入队
	// 测试失败一律不阻塞任务创建（失败只是不显示图标）
	host := extractHost(url)
	if host != "" {
		go m.prewarmCDN(task, host)
	}

	m.saveTasks() // 持久化：新建任务即落盘
	go m.schedule()

	return id
}

// extractHost 从 URL 抽取主机部分（scheme://host/path → host）
// 出错返回空字符串（调用方据此跳过预热）
func extractHost(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host
}

// prewarmCDN 对 host 做一次 HEAD 测试，记录响应时间和状态码到 task 字段
// 真实场景 CDN 通常会拒绝 HEAD（403/405），但 TCP 连接 + TLS 握手时间能反映链路质量。
// timeout 设为 3s，宁可预热失败也不阻塞任务创建。
func (m *TaskManager) prewarmCDN(t *Task, host string) {
	if t == nil || host == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	probe := &http.Client{
		Timeout: 3 * time.Second,
		// 不跟随重定向：CDN 预热只测链路，不去关心业务跳转
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	probeURL := "https://" + host + "/"
	req, err := http.NewRequestWithContext(ctx, "HEAD", probeURL, nil)
	if err != nil {
		m.updatePrewarm(t, "fail", 0, host)
		return
	}
	// 同样的 User-Agent 与指纹不能保证（downloader 用 uTLS），但通用 UA 即可触发 CDN
	// bot 检测也能大致反映真实下载路径的"能不能连上 + 多久握手"
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")

	start := time.Now()
	resp, err := probe.Do(req)
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		// 网络层失败（DNS / TCP / TLS / 超时）
		m.updatePrewarm(t, "fail", elapsed, host)
		return
	}
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 400:
		m.updatePrewarm(t, "ok", elapsed, host)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// 4xx 通常是 HEAD 被拒（405/403），但链路通了 → 标 warn
		m.updatePrewarm(t, "warn", elapsed, host)
	default:
		// 5xx 服务端异常
		m.updatePrewarm(t, "fail", elapsed, host)
	}
}

// updatePrewarm 原子地把预热结果写回 task（m.mu 加锁，保证前端轮询看到一致值）
func (m *TaskManager) updatePrewarm(t *Task, status string, ms int64, host string) {
	m.mu.Lock()
	t.PrewarmStatus = status
	t.PrewarmMs = ms
	t.PrewarmHost = host
	m.mu.Unlock()
}

// buildCmdString 生成展示用的命令字符串
func buildCmdString(cfg downloader.Config) string {
	parts := []string{"ddm3u8-go"}
	parts = append(parts, "-url", cfg.URL)
	parts = append(parts, "-save-dir", filepath.Dir(cfg.Output))
	parts = append(parts, "-save-name", strings.TrimSuffix(filepath.Base(cfg.Output), ".mp4"))
	parts = append(parts, "-concurrency", fmt.Sprintf("%d", cfg.Concurrency))
	parts = append(parts, "-fingerprint", cfg.Fingerprint)
	if cfg.Referer != "" {
		parts = append(parts, "-referer", cfg.Referer)
	}
	if cfg.UserAgent != "" {
		parts = append(parts, "-user-agent", cfg.UserAgent)
	}
	return strings.Join(parts, " ")
}

// schedule 调度任务执行（保证 maxParallel 并发上限）
func (m *TaskManager) schedule() {
	m.mu.Lock()
	active := 0
	for _, id := range m.order {
		t := m.tasks[id]
		if t.Status == StatusDownload || t.Status == StatusMerge {
			active++
		}
	}
	// 找到排队中的任务，在同一次锁内立即占位为"下载中"，
	// 避免 Unlock 后到 runTask 重新加锁设状态之间，被并发 schedule()
	// 重复选中同一任务（check-then-act 竞态：一次提交多个 URL 会并发触发多个 schedule，
	// 都读到同一 Queued 任务还未变 Downloading，导致对同一任务并发 runTask，
	// 两个 Downloader 写同一 TempDir 且后启动者覆盖 t.dl/t.cancel 使前者无法取消）
	var toStart *Task
	if active < m.maxParallel {
		for _, id := range m.order {
			t := m.tasks[id]
			if t.Status == StatusQueued {
				t.Status = StatusDownload // 占位，runTask 内会重新设置完整运行时字段
				toStart = t
				break
			}
		}
	}
	m.mu.Unlock()

	if toStart != nil {
		m.runTask(toStart)
		// 启动后递归再调度（可能还能再起一个）
		go m.schedule()
	}
}

// runTask 在 goroutine 中执行单个任务
func (m *TaskManager) runTask(t *Task) {
	ctx, cancel := context.WithCancel(context.Background())
	// 进度日志节流时间戳（原子读写，避免每个分片都抢 m.mu）
	// 用 atomic.Int64 而不是裸 int64：32-bit ARM 上 int64 栈变量不能保证 8 字节对齐，
	// 裸 atomic.LoadInt64/CompareAndSwapInt64 会触发同样的 unaligned 64-bit atomic panic。
	var lastProgressNanos atomic.Int64

	m.mu.Lock()
	t.cancel = cancel
	t.Status = StatusDownload
	t.startedAt = time.Now()
	// 启动时记录"下载命令"风格的日志（对齐 yt-dlp 版本的可观测性）
	t.Log = fmt.Sprintf("[调度器] 任务开始: URL=%s 输出=%s 指纹=%s 并发=%d 临时目录=%s",
		t.cfg.URL, t.cfg.Output, t.cfg.Fingerprint, t.cfg.Concurrency, t.cfg.TempDir)
	t.dl = downloader.New(t.cfg, func(format string, v ...interface{}) {
		msg := fmt.Sprintf(format, v...)
		// "progress done/total/failed" 是高频进度日志，原子节流避免每个分片都抢 m.mu：
		// 1000 分片 × 10 并发原本要 3000 次锁（m.mu + progressMu.RLock），
		// 节流后只在 500ms 间隔才抢一次 m.mu，CPU 大幅下降。
		if strings.HasPrefix(msg, "progress ") {
			now := time.Now().UnixNano()
			last := lastProgressNanos.Load()
			if now-last < int64(500*time.Millisecond) {
				return // 节流命中，不抢任何锁
			}
			// CAS 抢刷新权，避免多个 goroutine 同时写 t.Log
			if !lastProgressNanos.CompareAndSwap(last, now) {
				return
			}
			var done, total, failed int
			var bytes int64
			var speed float64
			_, _ = fmt.Sscanf(msg, "progress %d/%d/%d/%d/%f", &done, &total, &failed, &bytes, &speed)
			// 把"这一段完成"上报给 TaskManager 全局速度聚合器（不分流）
			// 即便当前节流命中（return）也要算全局——所以 Sscanf 之后无条件调用一次
			if done > 0 {
				// bytes 是累计已下载字节，估算本段字节：
				//   prevBytes ≈ bytes - 本段大小。done 也是累计完成数。
				//   但 done 可能是节流后的累计值，无法直接推出"上一次 to 这一次之间"增量。
				//   简化：用 speed × 节流间隔 估算瞬时分片字节数（节流固定 500ms）
				if speed > 0 {
					segBytes := int64(speed * 0.5) // 500ms 间隔
					if segBytes > 0 {
						m.recordGlobalSpeed(segBytes)
					}
				}
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			if total > 0 {
				logLine := fmt.Sprintf("下载中: %d/%d (%.0f%%)",
					done, total, float64(done)/float64(total)*100)
				// 基于已下载字节估算最终文件大小
				if done > 0 && bytes > 0 {
					avg := bytes / int64(done)
					estTotal := avg * int64(total)
					logLine += fmt.Sprintf(" | %s/%s",
						downloader.FormatBytes(bytes), downloader.FormatBytes(estTotal))
				}
				// 显示当前下载速度
				if speed > 0 {
					logLine += fmt.Sprintf(" | %s/s", downloader.FormatBytes(int64(speed)))
				}
				t.Log = logLine
			} else {
				t.Log = fmt.Sprintf("下载中: 已完成 %d", done)
			}
			return
		}
		// 其他日志（拉取 m3u8、合并、错误、seg 失败等低频）直接覆盖
		m.mu.Lock()
		defer m.mu.Unlock()
		t.Log = msg
	})
	// 注入握手失败回调：到阈值(5次)自动暂停，避免无限触发 CDN bot 风控
	t.dl.OnHandshakeFails = func() {
		m.Pause(t.ID)
	}
	// 主动掐检测：不再自动暂停（一个慢分片拖死整集太重），
	// 只在 downloader 内部把 abuseFails 置位，让后续分片 timeout 翻倍到 120s，
	// 给 CDN 一个"等流量过去"的窗口。任务继续跑，最终成功最好，
	// 真不行也会 retry 耗尽后正常失败，前端可手动暂停。
	t.dl.OnAbuseDetected = nil
	// 合并阶段开始回调：把 task.Status 切到"合并中"，前端立刻从"下载中 100%"
	// 切到"合并中"展示，避免老镜像"卡 100% 不变"的体验问题。
	t.dl.OnMergeStart = func() {
		m.mu.Lock()
		if t.Status == StatusDownload {
			t.Status = StatusMerge
		}
		m.mu.Unlock()
	}
	m.mu.Unlock()

	// 执行下载
	result, err := t.dl.Run(ctx)
	m.mu.Lock()
	// 先保存 dl 引用（lastNote 需要读 progress），再清空运行时字段
	dl := t.dl
	t.cancel = nil
	t.dl = nil

	if err != nil {
		// 若是 Pause 主动取消 ctx（status 已被设为 已暂停），保留已暂停状态，
		// 不覆盖为失败——这样前端能正确显示"已暂停"和"恢复+强合"按钮
		if t.Status == StatusPaused {
			m.mu.Unlock()
			m.logSegDurationSummary(t, dl) // 输出耗时分布，方便判断"主动掐 vs 慢网"
			m.saveTasks()                  // 持久化
			return
		}
		t.Status = StatusFailed
		t.Log = fmt.Sprintf("❌ %v  末尾输出: %s", err, lastNote(dl))
		m.mu.Unlock()
		m.logSegDurationSummary(t, dl) // 失败时也输出耗时分布
		// 失败时保留 temp_dir，方便点"强合"复用已下载分片
		m.saveTasks() // 持久化：状态变更
		return
	}
	t.Status = StatusDone
	t.OutputFile = result.OutputFile
	if result.Failed > 0 {
		// best-effort 完成：缺失分片已被黑场占位填充
		t.Log = fmt.Sprintf("✅ 完成(部分失败): %s", result.OutputFile)
	} else {
		t.Log = fmt.Sprintf("✅ 完成: %s", result.OutputFile)
	}
	tempDir := t.cfg.TempDir
	m.mu.Unlock()
	m.logSegDurationSummary(t, dl) // 成功时也输出耗时分布
	// 下载+合并均成功后清理 temp_dir（对齐 armv7l）
	if tempDir != "" {
		_ = os.RemoveAll(tempDir)
	}
	m.saveTasks() // 持久化：完成
}

// logSegDurationSummary 输出下载摘要到指定 task 的 t.Log
// 成功：总数/总耗时/平均速度
// 失败：总数/快速失败(<30s)占比/主要错误类型分布
// 帮助区分"主动掐流量"（<30s 快速失败集中）和"自然慢网"（耗时走满 60s）
// 注意：本函数内部自行加锁 m.mu，调用方必须未持有 m.mu，否则死锁
func (m *TaskManager) logSegDurationSummary(t *Task, dl *downloader.Downloader) {
	if t == nil || dl == nil {
		return
	}
	success, failed := dl.SegDurations()
	if len(success) == 0 && len(failed) == 0 {
		return
	}

	var lines []string

	// ---- 成功分片统计 ----
	if len(success) > 0 {
		var totalElapsed time.Duration
		var totalBytes int64
		var realCount int // elapsed > 0 的"实际下载"分片数（断点续传缓存的 elapsed=0 不计入平均）
		var realBytes int64
		for _, s := range success {
			totalBytes += s.Bytes
			if s.Elapsed > 0 {
				totalElapsed += s.Elapsed
				realBytes += s.Bytes
				realCount++
			}
		}
		avgElapsed := time.Duration(0)
		if realCount > 0 {
			avgElapsed = totalElapsed / time.Duration(realCount)
		}
		var avgSpeed float64
		if totalElapsed > 0 {
			avgSpeed = float64(realBytes) / totalElapsed.Seconds()
		}

		// 区分"实际下载"和"缓存命中"两个数字，便于断点续传场景诊断
		cachedCount := len(success) - realCount
		lines = append(lines, fmt.Sprintf(
			"[下载摘要] 成功 %d 个 (实际下载 %d + 缓存命中 %d) (总耗时 %s, 平均 %s/片, 平均速度 %s/s)",
			len(success),
			realCount, cachedCount,
			totalElapsed.Truncate(time.Millisecond),
			avgElapsed.Truncate(time.Millisecond),
			downloader.FormatBytes(int64(avgSpeed)),
		))
	}

	// ---- 失败分片统计 ----
	if len(failed) > 0 {
		fastFails := 0
		errCount := map[string]int{}
		for _, f := range failed {
			if f.Elapsed < 30*time.Second {
				fastFails++
			}
			// 错误信息归一化：去掉具体数字（如 seg 编号、port、字节数）便于聚合
			errCount[normalizeErr(f.Err)]++
		}
		// 取 Top 3 错误类型
		type kv struct {
			err string
			n   int
		}
		var top []kv
		for e, n := range errCount {
			top = append(top, kv{e, n})
		}
		sort.Slice(top, func(i, j int) bool { return top[i].n > top[j].n })
		if len(top) > 3 {
			top = top[:3]
		}
		var topStrs []string
		for _, k := range top {
			topStrs = append(topStrs, fmt.Sprintf("%s(%d)", k.err, k.n))
		}

		// 主动掐判定：快速失败占比 > 70% 且至少 3 个失败样本
		// 给出一行明确建议，让用户/前端日志区一眼看出是被掐了还是慢网
		hint := ""
		pct := fastFails * 100 / len(failed)
		if len(failed) >= 3 && pct >= 70 {
			hint = " ⚠️ 高度疑似主动掐流量"
		} else if pct >= 50 {
			hint = " (部分疑似主动掐)"
		}

		lines = append(lines, fmt.Sprintf(
			"[失败分片] %d 个 (快速失败<30s: %d 个, %d%%)%s | 主要错误: %s",
			len(failed), fastFails, pct, hint, strings.Join(topStrs, ", "),
		))
	}

	if len(lines) == 0 {
		return
	}

	// 追加到 t.Log（多行用 \n 拼接，前端日志区按行展示）
	m.mu.Lock()
	extra := strings.Join(lines, "\n")
	if t.Log != "" && !strings.HasSuffix(t.Log, "\n") {
		t.Log += "\n"
	}
	t.Log += extra
	m.mu.Unlock()
}

// normalizeErr 归一化错误信息，去掉易变部分（seg 编号、port、地址等）
// 让相似的错误能聚合到一起，而不是被具体数字打散
func normalizeErr(s string) string {
	if s == "" {
		return ""
	}
	// 截断到第一个 ":" 前（通常是 "context deadline exceeded" 这种）
	if i := strings.Index(s, ":"); i > 0 && i < 60 {
		return strings.TrimSpace(s[:i])
	}
	// 截断到第一个 "(" 或 "[" 前
	for _, sep := range []string{"(", "["} {
		if i := strings.Index(s, sep); i > 0 && i < 80 {
			return strings.TrimSpace(s[:i])
		}
	}
	if len(s) > 60 {
		return s[:60] + "..."
	}
	return s
}

// lastNote 返回下载器最近一条进度备注
func lastNote(d *downloader.Downloader) string {
	if d == nil {
		return ""
	}
	p := d.Progress()
	if p.Note != "" {
		return p.Note
	}
	return fmt.Sprintf("status=%s, total=%d, done=%d, failed=%d", p.Status, p.Total, p.Done, p.Failed)
}

// removeTempDir 删除任务的临时分片目录（幂等，目录不存在不报错）
// 对齐 armv7l：取消/删除/清理/完成/强合成功时都要清理缓存
func removeTempDir(t *Task) {
	if t == nil || t.cfg.TempDir == "" {
		return
	}
	_ = os.RemoveAll(t.cfg.TempDir)
}

// Pause 暂停任务：取消正在执行的下载 context，但保留 temp_dir 与已下载分片，
// 之后可点"恢复"断点续传。状态置为"已暂停"，对齐 armv7l 的 pause 语义。
func (m *TaskManager) Pause(id string) bool {
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return false
	}
	// 仅活跃任务可暂停（含音频提取 StatusConverting：杀掉 ffmpeg）
	if t.Status != StatusDownload && t.Status != StatusQueued && t.Status != StatusMerge && t.Status != StatusConverting {
		m.mu.Unlock()
		return false
	}
	if t.cancel != nil {
		t.cancel()
	}
	t.Status = StatusPaused
	if t.AudioTarget != nil {
		// 音频转换无断点：ffmpeg 被终止，恢复时从头重新转换
		t.Log = "已暂停（音频转换恢复将重新开始）"
	} else {
		t.Log = "已暂停，保留缓存可恢复"
	}
	m.mu.Unlock()
	// 注意：必须在锁外调用 saveTasks，否则 saveTasks 内部 RLock 会与
	// 当前 goroutine 持有的 Lock 死锁（Go RWMutex 不允许同 goroutine 既 Lock 又 RLock）
	m.saveTasks() // 持久化
	return true
}

// Cancel 取消任务（仅下载中/排队中/合并中/转换中），并清理 temp_dir
func (m *TaskManager) Cancel(id string) bool {
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return false
	}
	// 转换中（音频提取）也允许取消：杀掉 ffmpeg 进程
	if t.Status != StatusDownload && t.Status != StatusQueued && t.Status != StatusMerge && t.Status != StatusConverting {
		m.mu.Unlock()
		return false
	}
	if t.cancel != nil {
		t.cancel()
	}
	t.Status = StatusCanceled
	t.Log = "任务已取消"
	tempDir := t.cfg.TempDir
	m.mu.Unlock()
	// 在锁外做磁盘 IO
	if tempDir != "" {
		_ = os.RemoveAll(tempDir)
	}
	m.saveTasks() // 持久化
	return true
}

// Resume 恢复任务：重新入队（用于失败/取消/暂停后重启）
// 对齐 armv7l：已暂停也允许恢复，且 downloader 会跳过已存在的分片实现断点续传
func (m *TaskManager) Resume(id string) bool {
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return false
	}
	if t.Status != StatusFailed && t.Status != StatusCanceled && t.Status != StatusPaused && t.Status != StatusDone {
		m.mu.Unlock()
		return false
	}
	t.Status = StatusQueued
	t.Log = "等待执行（恢复）..."
	t.OutputFile = ""
	// audio_target 任务（音频转换）不走下载调度器：恢复=重新跑一次 ffmpeg 转换
	restartAudio := t.AudioTarget != nil
	m.mu.Unlock()

	m.saveTasks() // 持久化
	if restartAudio {
		go m.runAudioExtract(t)
	} else {
		go m.schedule()
	}
	return true
}

// Merge 强制合并：复用已下载的分片重新合并
// 适用于下载完成但合并失败的场景
func (m *TaskManager) Merge(id string) bool {
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return false
	}
	// 已完成 / 失败 / 已暂停 都允许强合（暂停时复用已下载的分片）
	if t.Status != StatusDone && t.Status != StatusFailed && t.Status != StatusPaused {
		m.mu.Unlock()
		return false
	}
	t.Status = StatusMerge
	t.Log = "强制合并中..."
	m.mu.Unlock()

	m.saveTasks() // 持久化
	go m.runMerge(t)
	return true
}

// runMerge 在 goroutine 中执行强制合并
func (m *TaskManager) runMerge(t *Task) {
	// 复用原 cfg 重新跑合并步骤
	// Downloader 的 Run 会重新下载分片，不适合强合
	// 这里直接调 ffmpeg concat 重新合并已有分片
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	t.cancel = cancel
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		t.cancel = nil
		m.mu.Unlock()
	}()

	// 直接用 downloader 的 merge 重新合并
	// 构造一个最小 Downloader 只为调 merge
	d := downloader.New(t.cfg, func(format string, v ...interface{}) {
		m.mu.Lock()
		t.Log = fmt.Sprintf(format, v...)
		m.mu.Unlock()
	})
	// 直接调 merge 需要重新解析 playlist（拿 segments 列表和 MapURI）
	// 简化处理：直接调用 ffmpeg concat 已有 .ts 文件
	err := d.MergeOnly(ctx, t.cfg.TempDir, t.cfg.Output)
	m.mu.Lock()
	if err != nil {
		t.Status = StatusFailed
		t.Log = fmt.Sprintf("❌ 强合失败: %v", err)
		m.mu.Unlock()
		// 强合失败保留 temp_dir，便于再次尝试
		m.saveTasks() // 持久化
		return
	}
	t.Status = StatusDone
	t.OutputFile = t.cfg.Output
	t.Log = fmt.Sprintf("✅ 强合完成: %s", t.cfg.Output)
	tempDir := t.cfg.TempDir
	m.mu.Unlock()
	// 强合成功后清理 temp_dir（对齐 armv7l）
	if tempDir != "" {
		_ = os.RemoveAll(tempDir)
	}
	m.saveTasks() // 持久化
}

// CreateLocalMerge 创建一个"本地缓存合并"任务（不重新下载，直接复用 tempDir 中已有的 .ts 分片）
// folderPath 是已存在分片的临时目录绝对路径，folderName 用于输出文件名与展示
// 命名规则：去掉 tempDir 名的 _temp 后缀，加 _merge，如 video_xxx_temp → video_xxx_merge.mp4
func (m *TaskManager) CreateLocalMerge(folderPath, folderName string) string {
	id := uuid.New().String()[:8]
	// 简化命名：去掉 _temp 后缀加 _merge，避免过长
	// 原 local_video_xxx_temp_yyy_zzz.mp4 → video_xxx_merge.mp4
	base := strings.TrimSuffix(folderName, "_temp")
	if base == folderName {
		// 没有 _temp 后缀，原样用
		base = folderName
	}
	fullName := base + "_merge"

	cfg := downloader.DefaultConfig()
	cfg.URL = "local://" + folderName // 占位，不参与下载
	cfg.FFmpegPath = m.ffmpegPath
	cfg.Fingerprint = m.fingerprint
	cfg.TempDir = folderPath
	cfg.Output = filepath.Join(m.downloadDir, fullName+".mp4")
	cfg.NoMerge = false // 走 MergeOnly

	task := &Task{
		ID:        id,
		Name:      fullName,
		URL:       cfg.URL,
		Status:    StatusMerge,
		Log:       "强制合并中（本地缓存）...",
		CreatedAt: time.Now(),
		cfg:       cfg,
		// 对外可见字段：与 armv7l Python 版对齐（folder_target 标识本地合并源目录）
		DownloadDir:  m.downloadDir,
		TempDir:      folderPath,
		FolderTarget: folderName,
	}
	task.cmd = fmt.Sprintf("ddm3u8-go --merge-only --temp-dir %s --output %s", folderPath, cfg.Output)

	m.mu.Lock()
	m.tasks[id] = task
	m.order = append([]string{id}, m.order...)
	m.mu.Unlock()

	m.saveTasks() // 持久化
	// 直接进入合并流程（不走 schedule/runTask，避免重新下载）
	go m.runMerge(task)
	return id
}

// CreateAudioExtract 创建一个"音频提取"任务：调用 ffmpeg 把视频文件转成 .m4a 音频。
// inputPath / outputPath 是绝对路径；ffmpegPath 用于指定 ffmpeg 二进制。
// 任务会进入 schedule 自动运行（与下载任务共用并发槽）。
func (m *TaskManager) CreateAudioExtract(inputPath, outputPath, ffmpegPath string) string {
	id := uuid.New().String()[:8]
	baseName := filepath.Base(inputPath)
	ext := filepath.Ext(baseName)
	nameOnly := strings.TrimSuffix(baseName, ext)
	ts := time.Now().Format("0102_150405")
	taskName := fmt.Sprintf("%s_%s_%s", nameOnly, ts, id[:3])

	task := &Task{
		ID:        id,
		Name:      taskName,
		URL:       "音频提取: " + baseName,
		Status:    StatusQueued,
		Log:       "等待转换...",
		CreatedAt: time.Now(),
		cfg: downloader.Config{
			FFmpegPath: ffmpegPath,
		},
		// 对外可见字段：与 armv7l Python 版 audio_target 同形
		DownloadDir: filepath.Dir(inputPath),
		AudioTarget: &AudioTarget{
			InputPath:  inputPath,
			OutputPath: outputPath,
		},
	}
	task.cmd = fmt.Sprintf("ffmpeg -y -i %s -vn -acodec aac -b:a 192k %s", inputPath, outputPath)

	m.mu.Lock()
	m.tasks[id] = task
	m.order = append([]string{id}, m.order...)
	m.mu.Unlock()

	m.saveTasks() // 持久化
	go m.runAudioExtract(task)
	return id
}

// runAudioExtract 跑 ffmpeg 提取音频；同时解析 -progress 输出，
// 把「百分比 / 已用时间 / 总时长 / 速度 / 输出大小」写入 t.Log，前端
// 进度条会通过 t.Log 中的 "X%" 自动渲染。
//
// 设计要点：
//   - ffmpeg 加 -progress pipe:1 -nostats：stdout 输出干净的 key=value 行
//     （out_time, speed, total_size, progress=continue|end），stderr 仅用于
//     解析初始 "Duration: HH:MM:SS.xx" 拿总时长
//   - 进度更新限流到 500ms 一次：前端轮询 2s/次，没必要每 50ms 都写锁；
//     也避免 t.Log 抖动干扰视觉
func (m *TaskManager) runAudioExtract(t *Task) {
	if t.AudioTarget == nil {
		return
	}
	inputPath := t.AudioTarget.InputPath
	outputPath := t.AudioTarget.OutputPath
	ffmpegPath := t.cfg.FFmpegPath
	if ffmpegPath == "" {
		ffmpegPath = "ffmpeg"
	}

	// 转"转换中" + 注册 cancel
	m.mu.Lock()
	t.Status = StatusConverting
	if t.cancel != nil {
		t.cancel()
	}
	t.cancel = nil
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	m.mu.Unlock()
	defer cancel()

	t.Log = "开始提取音频（解析时长）..."
	m.saveTasks()

	cmd := exec.CommandContext(ctx, ffmpegPath,
		"-y", "-nostats", "-progress", "pipe:1",
		"-i", inputPath,
		"-vn", "-acodec", "aac", "-b:a", "192k",
		outputPath,
	)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		m.mu.Lock()
		t.Status = StatusFailed
		t.Log = fmt.Sprintf("❌ ffmpeg 启动失败: %v", err)
		m.mu.Unlock()
		m.saveTasks()
		return
	}

	// 总时长（秒，浮点）；stderr 解析后写入，多 goroutine 只写一次
	var totalDurSec atomic.Pointer[float64]
	durRe := regexp.MustCompile(`Duration:\s+(\d+):(\d+):(\d+(?:\.\d+)?)`)

	// stderr：解析 Duration（ffmpeg 启动早期会往 stderr 打这一行）
	go func() {
		scanner := bufio.NewScanner(stderr)
		// 进度信息不带 "Duration:" 时长=0 字节，行长通常 < 1KB，64KB 缓冲足够
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if dm := durRe.FindStringSubmatch(line); dm != nil && totalDurSec.Load() == nil {
				h, _ := strconv.ParseFloat(dm[1], 64)
				min, _ := strconv.ParseFloat(dm[2], 64)
				sec, _ := strconv.ParseFloat(dm[3], 64)
				d := h*3600 + min*60 + sec
				totalDurSec.Store(&d)
				// 拿到总时长后立刻刷一次 log，让前端立刻有百分比而不是 0%
				m.mu.Lock()
				t.Log = fmt.Sprintf("[音频提取] 0.0%% | 00:00:00 / %s | 速度 N/A | 输出 0 B",
					formatHmsFloat(*totalDurSec.Load()))
				m.mu.Unlock()
			}
		}
	}()

	// stdout：解析 key=value 进度
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		var outSec float64
		var totalSize int64
		var speedX float64
		var lastLog time.Time
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			key, val, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			switch key {
			case "out_time":
				// "HH:MM:SS.uuuuuu"
				if v, err := parseHms(val); err == nil {
					outSec = v
				}
			case "total_size":
				if v, err := strconv.ParseInt(val, 10, 64); err == nil {
					totalSize = v
				}
			case "speed":
				// "1.5x" 或 "N/A"
				if strings.HasSuffix(val, "x") {
					if f, err := strconv.ParseFloat(strings.TrimSuffix(val, "x"), 64); err == nil {
						speedX = f
					}
				}
			case "progress":
				// 限流：500ms 内只更新一次
				if val != "continue" || time.Since(lastLog) < 500*time.Millisecond {
					continue
				}
				lastLog = time.Now()
				var pct float64
				var totalPtr = totalDurSec.Load()
				if totalPtr != nil && *totalPtr > 0 {
					pct = outSec / *totalPtr * 100
					if pct > 100 {
						pct = 100
					}
				}
				logLine := fmt.Sprintf("[音频提取] %.1f%% | %s / %s | 速度 %s | 输出 %s",
					pct,
					formatHmsFloat(outSec),
					formatHmsPtr(totalDurSec.Load()),
					formatSpeedX(speedX),
					downloader.FormatBytes(totalSize),
				)
				m.mu.Lock()
				t.Log = logLine
				m.mu.Unlock()
			}
		}
	}()

	err := cmd.Wait()
	m.mu.Lock()
	if err != nil {
		// ffmpeg 被 pause/cancel 主动终止（ctx 取消）：Pause/Cancel 已把状态改成
		// "已暂停"/"已取消"，这里不能覆盖成"失败"。
		if ctx.Err() == nil {
			t.Status = StatusFailed
			t.Log = fmt.Sprintf("❌ FFmpeg转换失败(退出码:%d)", cmd.ProcessState.ExitCode())
		}
	} else {
		t.Status = "完成(音频)"
		t.Log = "✅ 音频提取成功: " + filepath.Base(outputPath)
	}
	t.cancel = nil
	m.mu.Unlock()
	m.saveTasks()
}

// parseHms 解析 ffmpeg "HH:MM:SS.uuuuuu" / "Duration: HH:MM:SS.xx" 格式，返回秒
func parseHms(s string) (float64, error) {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time: %q", s)
	}
	h, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, err
	}
	min, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, err
	}
	sec, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return 0, err
	}
	return h*3600 + min*60 + sec, nil
}

// formatHmsFloat 把秒数格式化为 HH:MM:SS（用于已用时间）
func formatHmsFloat(sec float64) string {
	if sec < 0 {
		sec = 0
	}
	total := int64(sec)
	h := total / 3600
	m := (total % 3600) / 60
	s := total % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// formatHmsPtr 把 *float64 秒数格式化为 HH:MM:SS，nil 时返回 "--:--:--"
func formatHmsPtr(sec *float64) string {
	if sec == nil {
		return "--:--:--"
	}
	return formatHmsFloat(*sec)
}

// formatSpeedX 格式化 ffmpeg speed 值（1.5 → "1.5x"，0 → "N/A"）
func formatSpeedX(s float64) string {
	if s <= 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.1fx", s)
}

// Delete 删除任务记录（不能删除活跃中的）；同时清理 temp_dir
func (m *TaskManager) Delete(id string) bool {
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return false
	}
	if t.Status == StatusDownload || t.Status == StatusQueued || t.Status == StatusMerge {
		m.mu.Unlock()
		return false
	}
	tempDir := t.cfg.TempDir
	delete(m.tasks, id)
	for i, x := range m.order {
		if x == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.mu.Unlock()
	// 在锁外做磁盘 IO：删除残留缓存
	if tempDir != "" {
		_ = os.RemoveAll(tempDir)
	}
	m.saveTasks() // 持久化
	return true
}

// Clear 清除所有非活跃任务；同时清理各自的 temp_dir
func (m *TaskManager) Clear() int {
	m.mu.Lock()
	var tempDirs []string
	n := 0
	for id, t := range m.tasks {
		if t.Status == StatusDownload || t.Status == StatusQueued || t.Status == StatusMerge {
			continue
		}
		if t.cfg.TempDir != "" {
			tempDirs = append(tempDirs, t.cfg.TempDir)
		}
		delete(m.tasks, id)
		n++
	}
	// 重建 order
	newOrder := []string{}
	for _, id := range m.order {
		if _, ok := m.tasks[id]; ok {
			newOrder = append(newOrder, id)
		}
	}
	m.order = newOrder
	m.mu.Unlock()
	// 在锁外做磁盘 IO
	for _, d := range tempDirs {
		_ = os.RemoveAll(d)
	}
	m.saveTasks() // 持久化
	return n
}

// Snapshot 返回所有任务的快照（按 order 排序）+ 活跃 worker 数
type Snapshot struct {
	Tasks         map[string]TaskView `json:"tasks"`
	TaskOrder     []string            `json:"task_order"`
	ActiveWorkers int                 `json:"active_workers"`
	MaxWorkers    int                 `json:"max_workers"`
}

// TaskView 是对外暴露的任务视图（去掉内部字段）
//
// 字段命名与 armv7l 分支 Python 版严格对齐：download_dir / temp_dir / headers /
// folder_target / audio_target / cmd_str。这样外部中间件可以无缝对接 go-core 后端。
type TaskView struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	URL          string            `json:"url"`
	Status       string            `json:"status"`
	Log          string            `json:"log"`
	Cmd          string            `json:"cmd,omitempty"`
	CmdStr       string            `json:"cmd_str,omitempty"` // 与 Python 版兼容（Python 区分 cmd list 与 cmd_str 字符串）
	OutputFile   string            `json:"output_file,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	DownloadDir  string            `json:"download_dir,omitempty"`
	TempDir      string            `json:"temp_dir,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	FolderTarget string            `json:"folder_target,omitempty"`
	AudioTarget  *AudioTarget      `json:"audio_target,omitempty"`
	Progress     ProgressView      `json:"progress"`

	// CDN 预热（任务开始时对 m3u8 主机做 HEAD 测速）
	PrewarmStatus string `json:"prewarm_status,omitempty"`
	PrewarmMs     int64  `json:"prewarm_ms,omitempty"`
	PrewarmHost   string `json:"prewarm_host,omitempty"`
}

// ProgressView 进度视图（映射 downloader.Progress）
type ProgressView struct {
	Total   int    `json:"total"`
	Done    int    `json:"done"`
	Failed  int    `json:"failed"`
	Current int    `json:"current"`
	Status  string `json:"status"`
}

// Snapshot 返回当前快照
func (m *TaskManager) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	active := 0
	tasksView := map[string]TaskView{}
	for _, id := range m.order {
		t := m.tasks[id]
		if t.Status == StatusDownload || t.Status == StatusMerge {
			active++
		}
		v := TaskView{
			ID:           t.ID,
			Name:         t.Name,
			URL:          t.URL,
			Status:       t.Status,
			Log:          t.Log,
			Cmd:          t.cmd,
			CmdStr:       t.cmd, // 与 armv7l Python 版同名同义
			OutputFile:   t.OutputFile,
			CreatedAt:    t.CreatedAt,
			DownloadDir:  t.DownloadDir,
			TempDir:      t.TempDir,
			Headers:      t.Headers,
			FolderTarget: t.FolderTarget,
			AudioTarget:  t.AudioTarget,
			PrewarmStatus: t.PrewarmStatus,
			PrewarmMs:     t.PrewarmMs,
			PrewarmHost:   t.PrewarmHost,
		}
		if t.dl != nil {
			p := t.dl.Progress()
			v.Progress = ProgressView{
				Total: p.Total, Done: p.Done, Failed: p.Failed,
				Current: p.Current, Status: p.Status,
			}
		}
		tasksView[id] = v
	}
	return Snapshot{
		Tasks: tasksView, TaskOrder: append([]string(nil), m.order...),
		ActiveWorkers: active, MaxWorkers: m.maxParallel,
	}
}

// Get 返回单个任务（含详细调试信息）
func (m *TaskManager) Get(id string) (TaskView, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	if !ok {
		return TaskView{}, false
	}
	v := TaskView{
		ID: t.ID, Name: t.Name, URL: t.URL, Status: t.Status,
		Log: t.Log, Cmd: t.cmd, CmdStr: t.cmd,
		OutputFile: t.OutputFile, CreatedAt: t.CreatedAt,
		DownloadDir: t.DownloadDir, TempDir: t.TempDir,
		Headers: t.Headers, FolderTarget: t.FolderTarget,
		AudioTarget: t.AudioTarget,
		PrewarmStatus: t.PrewarmStatus,
		PrewarmMs:     t.PrewarmMs,
		PrewarmHost:   t.PrewarmHost,
	}
	if t.dl != nil {
		p := t.dl.Progress()
		v.Progress = ProgressView{
			Total: p.Total, Done: p.Done, Failed: p.Failed,
			Current: p.Current, Status: p.Status,
		}
	}
	return v, true
}

// MaskCookieInHeaders 复制 headers 并把 Cookie 值脱敏为前 20 字符 + "..."，
// 与 armv7l 分支 Python 版 /api/task/{id}/debug 行为一致。
// 用于暴露给前端的调试接口，避免 Cookie 完整泄露。
func MaskCookieInHeaders(h map[string]string) map[string]string {
	if h == nil {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		if strings.EqualFold(k, "Cookie") && len(v) > 20 {
			out[k] = v[:20] + "...(已脱敏)"
		} else {
			out[k] = v
		}
	}
	return out
}

// ListVideoFiles 扫描下载目录里的视频文件
// 带 5s TTL 缓存：避免前端高频轮询时每次都全盘 Walk（armv7l 上百毫秒级 IO）
func (m *TaskManager) ListVideoFiles() []map[string]interface{} {
	m.listCacheMu.Lock()
	if time.Since(m.listCacheAt) < listCacheTTL && m.listCacheVideos != nil {
		cached := m.listCacheVideos
		m.listCacheMu.Unlock()
		return cached
	}
	m.listCacheMu.Unlock()

	videos := []map[string]interface{}{}
	videoExt := map[string]bool{".mp4": true, ".ts": true, ".mkv": true, ".m4a": true, ".avi": true}
	err := filepath.Walk(m.downloadDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasPrefix(info.Name(), ".") {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(info.Name()))
		if !videoExt[ext] {
			return nil
		}
		rel, _ := filepath.Rel(m.downloadDir, path)
		videos = append(videos, map[string]interface{}{
			"name": info.Name(),
			"path": strings.ReplaceAll(rel, "\\", "/"),
			"size": info.Size(),
		})
		return nil
	})
	_ = err

	m.listCacheMu.Lock()
	m.listCacheVideos = videos
	m.listCacheAt = time.Now()
	m.listCacheMu.Unlock()
	return videos
}

// ListFolders 扫描一层 + 二层子目录
// 与 ListVideoFiles 共享 5s TTL 缓存。
func (m *TaskManager) ListFolders() []string {
	m.listCacheMu.Lock()
	if time.Since(m.listCacheAt) < listCacheTTL && m.listCacheFolds != nil {
		cached := m.listCacheFolds
		m.listCacheMu.Unlock()
		return cached
	}
	m.listCacheMu.Unlock()

	folders := []string{}
	entries, err := os.ReadDir(m.downloadDir)
	if err != nil {
		return folders
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		folders = append(folders, e.Name())
		subPath := filepath.Join(m.downloadDir, e.Name())
		subs, err := os.ReadDir(subPath)
		if err != nil {
			continue
		}
		for _, s := range subs {
			if !s.IsDir() || strings.HasPrefix(s.Name(), ".") {
				continue
			}
			folders = append(folders, e.Name()+"/"+s.Name())
		}
	}

	m.listCacheMu.Lock()
	m.listCacheFolds = folders
	m.listCacheAt = time.Now()
	m.listCacheMu.Unlock()
	return folders
}

// ============ 任务历史持久化（对齐 armv7l：JSON 文件，原子写） ============

// taskRecord 是任务的可序列化形式（剥离运行时字段：cfg/cancel/dl/startedAt）
type taskRecord struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	URL        string    `json:"url"`
	Status     string    `json:"status"`
	Log        string    `json:"log"`
	Cmd        string    `json:"cmd,omitempty"`
	OutputFile string    `json:"output_file,omitempty"`
	CreatedAt  time.Time `json:"created_at"`

	// cfg 的关键字段，用于重启后重建 downloader.Config（断点续传）
	CfgURL         string `json:"cfg_url"`
	CfgOutput      string `json:"cfg_output"`
	CfgTempDir     string `json:"cfg_temp_dir"`
	CfgFingerprint string `json:"cfg_fingerprint"`
	CfgReferer     string `json:"cfg_referer,omitempty"`
	CfgUserAgent   string `json:"cfg_user_agent,omitempty"`

	// 对外可见字段：与 armv7l Python 版持久化同形，重启后中间件仍可读到
	DownloadDir  string            `json:"download_dir,omitempty"`
	TempDir      string            `json:"temp_dir,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	FolderTarget string            `json:"folder_target,omitempty"`
	AudioTarget  *AudioTarget      `json:"audio_target,omitempty"`
}

// saveTasks 把所有任务快照写盘（原子写：tmp + rename）。
// 调用方必须已持有合适的锁（或快照已拷贝），本函数不再加 m.mu。
// 在锁外执行文件 IO，避免持久化阻塞调度。
// saveMu 串行化：多个协程并发调 saveTasks 时只允许一个实际执行 IO，
// 避免重复 json.Marshal + WriteFile 浪费 CPU/IO（批量 Delete 时尤其明显）。
// 空 dbPath 时为 no-op。
func (m *TaskManager) saveTasks() {
	if m.dbPath == "" {
		return
	}
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	m.mu.RLock()
	// 拷贝一份快照，尽快释放锁
	records := make([]taskRecord, 0, len(m.tasks))
	for _, t := range m.tasks {
		records = append(records, taskRecord{
			ID: t.ID, Name: t.Name, URL: t.URL, Status: t.Status,
			Log: t.Log, Cmd: t.cmd, OutputFile: t.OutputFile,
			CreatedAt:      t.CreatedAt,
			CfgURL:         t.cfg.URL,
			CfgOutput:      t.cfg.Output,
			CfgTempDir:     t.cfg.TempDir,
			CfgFingerprint: t.cfg.Fingerprint,
			CfgReferer:     t.cfg.Referer,
			CfgUserAgent:   t.cfg.UserAgent,
			DownloadDir:    t.DownloadDir,
			TempDir:        t.TempDir,
			Headers:        t.Headers,
			FolderTarget:   t.FolderTarget,
			AudioTarget:    t.AudioTarget,
		})
	}
	dbPath := m.dbPath
	m.mu.RUnlock()

	tmp := dbPath + ".tmp"
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return
	}
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, dbPath)
}

// LoadTasks 从 dbPath 加载任务历史（启动时调用）。
// 活跃状态（下载中/合并中/排队中）降级为"已中断"，可点恢复重启。
// 非活跃状态（已完成/失败/取消/暂停）原样保留。
func (m *TaskManager) LoadTasks() {
	if m.dbPath == "" {
		return
	}
	data, err := os.ReadFile(m.dbPath)
	if err != nil {
		// 文件不存在（首次启动）属正常，不报错
		return
	}
	var records []taskRecord
	if err := json.Unmarshal(data, &records); err != nil {
		// 损坏的 db：备份后丢弃，避免反复读坏文件
		bak := m.dbPath + ".corrupt"
		_ = os.Rename(m.dbPath, bak)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// 按 createdAt 升序恢复，再反转入 order（最新在前）
	for _, r := range records {
		t := &Task{
			ID: r.ID, Name: r.Name, URL: r.URL, Status: r.Status,
			Log: r.Log, cmd: r.Cmd, OutputFile: r.OutputFile,
			CreatedAt:   r.CreatedAt,
			DownloadDir: r.DownloadDir, TempDir: r.TempDir,
			Headers: r.Headers, FolderTarget: r.FolderTarget,
			AudioTarget: r.AudioTarget,
		}
		// 重建 cfg（用于恢复/强合）
		t.cfg = downloader.DefaultConfig()
		t.cfg.URL = r.CfgURL
		t.cfg.Output = r.CfgOutput
		t.cfg.TempDir = r.CfgTempDir
		t.cfg.Fingerprint = r.CfgFingerprint
		t.cfg.Referer = r.CfgReferer
		t.cfg.UserAgent = r.CfgUserAgent
		t.cfg.FFmpegPath = m.ffmpegPath
		// 活跃状态降级：重启后无法续跑正在进行的 ctx，标记为中断
		if t.Status == StatusDownload || t.Status == StatusMerge || t.Status == StatusQueued {
			t.Status = StatusFailed
			t.Log = "系统重启导致中断，可点恢复"
		}
		m.tasks[t.ID] = t
	}
	// order 按创建时间倒序
	m.order = m.order[:0]
	for id := range m.tasks {
		m.order = append(m.order, id)
	}
	// 反转使最新在前（记录是任意顺序，按 CreatedAt 排）
	for i, j := 0, len(m.order)-1; i < j; i, j = i+1, j-1 {
		m.order[i], m.order[j] = m.order[j], m.order[i]
	}
	// 用 CreatedAt 稳定排序
	type kv struct {
		id string
		ts time.Time
	}
	tmp := make([]kv, len(m.order))
	for i, id := range m.order {
		tmp[i] = kv{id, m.tasks[id].CreatedAt}
	}
	for i := 0; i < len(tmp); i++ {
		for j := i + 1; j < len(tmp); j++ {
			if tmp[j].ts.After(tmp[i].ts) {
				tmp[i], tmp[j] = tmp[j], tmp[i]
			}
		}
	}
	for i := range m.order {
		m.order[i] = tmp[i].id
	}
}

// CleanupOrphanTempDirs 启动时清理无任务对应的 _temp 目录（对齐 armv7l）。
// 留下的活跃任务 temp_dir 不清，避免误删正在用的分片。
func (m *TaskManager) CleanupOrphanTempDirs() int {
	entries, err := os.ReadDir(m.downloadDir)
	if err != nil {
		return 0
	}
	m.mu.RLock()
	active := map[string]bool{}
	for _, t := range m.tasks {
		if t.cfg.TempDir != "" {
			active[filepath.Base(t.cfg.TempDir)] = true
		}
	}
	m.mu.RUnlock()
	cleaned := 0
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || !strings.HasSuffix(name, "_temp") {
			continue
		}
		if active[name] {
			continue
		}
		_ = os.RemoveAll(filepath.Join(m.downloadDir, name))
		cleaned++
	}
	return cleaned
}
