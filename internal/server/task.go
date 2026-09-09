// Package server 提供 Web 服务，API 兼容原 Flask 版。
package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/pelico/DDM3U8-go/internal/downloader"
)

// 任务状态常量（与原 Flask 前端展示对齐）
const (
	StatusQueued   = "排队中"
	StatusDownload = "下载中"
	StatusMerge    = "合并中"
	StatusDone     = "已完成"
	StatusFailed   = "失败"
	StatusCanceled = "已取消"
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
	cfg      downloader.Config
	cancel   context.CancelFunc
	dl       *downloader.Downloader
	startedAt time.Time
	cmd      string
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
}

// NewTaskManager 创建任务管理器
func NewTaskManager(maxParallel int, downloadDir, tempBaseDir, ffmpegPath, fingerprint string) *TaskManager {
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
	}
}

// Create 创建新任务（使用默认下载目录）
func (m *TaskManager) Create(url, name string, headers map[string]string) string {
	return m.CreateWithDir(url, name, headers, m.downloadDir)
}

// CreateWithDir 创建新任务（指定下载目录，用于 sub_path）
func (m *TaskManager) CreateWithDir(url, name string, headers map[string]string, downloadDir string) string {
	id := uuid.New().String()[:8]
	ts := time.Now().Format("0102_150405")
	fullName := fmt.Sprintf("%s_%s_%s", name, ts, id[:3])

	cfg := downloader.DefaultConfig()
	cfg.URL = url
	cfg.FFmpegPath = m.ffmpegPath
	cfg.Fingerprint = m.fingerprint
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
	}
	task.cmd = buildCmdString(cfg)

	m.mu.Lock()
	m.tasks[id] = task
	m.order = append([]string{id}, m.order...)
	m.mu.Unlock()

	// 触发调度
	go m.schedule()

	return id
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
	// 找到排队中的任务，启动执行
	var toStart *Task
	for _, id := range m.order {
		t := m.tasks[id]
		if t.Status == StatusQueued {
			toStart = t
			break
		}
	}
	m.mu.Unlock()

	if toStart != nil && active < m.maxParallel {
		m.runTask(toStart)
		// 启动后递归再调度（可能还能再起一个）
		go m.schedule()
	}
}

// runTask 在 goroutine 中执行单个任务
func (m *TaskManager) runTask(t *Task) {
	ctx, cancel := context.WithCancel(context.Background())

	m.mu.Lock()
	t.cancel = cancel
	t.Status = StatusDownload
	t.startedAt = time.Now()
	// 启动时记录"下载命令"风格的日志（对齐 yt-dlp 版本的可观测性）
	t.Log = fmt.Sprintf("[调度器] 任务开始: URL=%s 输出=%s 指纹=%s 并发=%d 临时目录=%s",
		t.cfg.URL, t.cfg.Output, t.cfg.Fingerprint, t.cfg.Concurrency, t.cfg.TempDir)
	t.dl = downloader.New(t.cfg, func(format string, v ...interface{}) {
		// 关键日志写入 t.Log，但对 "seg N ok" 这类高频日志做摘要
		msg := fmt.Sprintf(format, v...)
		m.mu.Lock()
		defer m.mu.Unlock()
		// "seg N ok" 这种进度日志：显示成 "已完成 N/Total (失败 F)"
		if strings.HasPrefix(msg, "seg ") && strings.HasSuffix(msg, " ok") {
			p := t.dl.Progress()
			if p.Total > 0 {
				t.Log = fmt.Sprintf("下载中: %d/%d (失败 %d, %.0f%%)",
					p.Done, p.Total, p.Failed,
					float64(p.Done)/float64(p.Total)*100)
			} else {
				t.Log = fmt.Sprintf("下载中: 已完成 %d (失败 %d)", p.Done, p.Failed)
			}
			return
		}
		// 其他日志（拉取 m3u8、合并、错误等）直接覆盖
		t.Log = msg
	})
	m.mu.Unlock()

	// 执行下载
	result, err := t.dl.Run(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	t.cancel = nil
	t.dl = nil

	if err != nil {
		t.Status = StatusFailed
		t.Log = fmt.Sprintf("❌ %v  末尾输出: %s", err, lastNote(t.dl))
		return
	}
	t.Status = StatusDone
	t.OutputFile = result.OutputFile
	t.Log = fmt.Sprintf("✅ 完成: %s (%d 分片, 失败 %d, 耗时 %s)",
		result.OutputFile, result.Segments, result.Failed, result.Duration.Truncate(time.Millisecond))
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

// Cancel 取消任务（仅下载中/排队中）
func (m *TaskManager) Cancel(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return false
	}
	if t.Status != StatusDownload && t.Status != StatusQueued && t.Status != StatusMerge {
		return false
	}
	if t.cancel != nil {
		t.cancel()
	}
	t.Status = StatusCanceled
	t.Log = "已取消"
	return true
}

// Resume 恢复任务：重新入队（用于失败/取消后重启）
func (m *TaskManager) Resume(id string) bool {
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return false
	}
	if t.Status != StatusFailed && t.Status != StatusCanceled && t.Status != StatusDone {
		m.mu.Unlock()
		return false
	}
	t.Status = StatusQueued
	t.Log = "等待执行（恢复）..."
	t.OutputFile = ""
	m.mu.Unlock()

	go m.schedule()
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
	// 下载已完成（Done/Failed）才能强合
	if t.Status != StatusDone && t.Status != StatusFailed {
		m.mu.Unlock()
		return false
	}
	t.Status = StatusMerge
	t.Log = "强制合并中..."
	m.mu.Unlock()

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
	defer m.mu.Unlock()
	if err != nil {
		t.Status = StatusFailed
		t.Log = fmt.Sprintf("❌ 强合失败: %v", err)
		return
	}
	t.Status = StatusDone
	t.OutputFile = t.cfg.Output
	t.Log = fmt.Sprintf("✅ 强合完成: %s", t.cfg.Output)
}

// Delete 删除任务记录（不能删除活跃中的）
func (m *TaskManager) Delete(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return false
	}
	if t.Status == StatusDownload || t.Status == StatusQueued || t.Status == StatusMerge {
		return false
	}
	delete(m.tasks, id)
	for i, x := range m.order {
		if x == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	return true
}

// Clear 清除所有非活跃任务
func (m *TaskManager) Clear() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for id, t := range m.tasks {
		if t.Status == StatusDownload || t.Status == StatusQueued || t.Status == StatusMerge {
			continue
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
type TaskView struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	URL        string    `json:"url"`
	Status     string    `json:"status"`
	Log        string    `json:"log"`
	Cmd        string    `json:"cmd,omitempty"`
	OutputFile string    `json:"output_file,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	Progress   ProgressView `json:"progress"`
}

// ProgressView 进度视图（映射 downloader.Progress）
type ProgressView struct {
	Total  int    `json:"total"`
	Done   int    `json:"done"`
	Failed int    `json:"failed"`
	Current int   `json:"current"`
	Status string `json:"status"`
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
			ID:        t.ID,
			Name:      t.Name,
			URL:       t.URL,
			Status:    t.Status,
			Log:       t.Log,
			Cmd:       t.cmd,
			OutputFile: t.OutputFile,
			CreatedAt: t.CreatedAt,
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
		Log: t.Log, Cmd: t.cmd, OutputFile: t.OutputFile,
		CreatedAt: t.CreatedAt,
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

// ListVideoFiles 扫描下载目录里的视频文件
func (m *TaskManager) ListVideoFiles() []map[string]interface{} {
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
	return videos
}

// ListFolders 扫描一层 + 二层子目录
func (m *TaskManager) ListFolders() []string {
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
	return folders
}
