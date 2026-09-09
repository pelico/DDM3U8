package server

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Server Web 服务
type Server struct {
	tm     *TaskManager
	cfg    Config
	mux    *http.ServeMux
}

// Config Web 服务配置
type Config struct {
	Port         string
	DownloadDir  string
	TempBaseDir  string
	FFmpegPath   string
	Fingerprint  string
	MaxParallel  int
	WebUser      string
	WebPass      string
	// DBPath 任务历史持久化路径（JSON 文件），空则不持久化
	DBPath       string
	// TemplatesFS 前端静态资源（embed 进二进制）
	TemplatesFS  embed.FS
}

// New 创建 Web 服务
func New(cfg Config) *Server {
	tm := NewTaskManager(
		cfg.MaxParallel, cfg.DownloadDir, cfg.TempBaseDir,
		cfg.FFmpegPath, cfg.Fingerprint, cfg.DBPath,
	)
	// 启动时加载任务历史：活跃状态降为"已中断"，非活跃保留
	tm.LoadTasks()
	// 清理无任务对应的孤儿 _temp 目录
	if n := tm.CleanupOrphanTempDirs(); n > 0 {
		log.Printf("[启动清理] 清理了 %d 个残留临时目录", n)
	}
	s := &Server{tm: tm, cfg: cfg, mux: http.NewServeMux()}
	s.routes()
	return s
}

// routes 注册所有路由（API 对齐原 Flask 版）
func (s *Server) routes() {
	// 静态首页（embed 的 templates/index.html）
	s.mux.HandleFunc("/", s.withAuth(s.index))

	// 任务管理
	s.mux.HandleFunc("/api/tasks", s.withAuth(s.tasksHandler))
	s.mux.HandleFunc("/api/task/", s.withAuth(s.taskActionHandler)) // /api/task/{id} 和 /api/task/{id}/debug
	s.mux.HandleFunc("/api/clear", s.withAuth(s.clearHandler))
	s.mux.HandleFunc("/api/clear-selected", s.withAuth(s.clearSelectedHandler))

	// 文件浏览
	s.mux.HandleFunc("/api/folders", s.withAuth(s.foldersHandler))
	s.mux.HandleFunc("/api/video_files", s.withAuth(s.videoFilesHandler))

	// 创建下载任务（与 Flask /down 完全兼容）
	s.mux.HandleFunc("/down", s.withAuth(s.downHandler))

	// 本地缓存合并（复用 /downloads 下的 .ts 分片，不重新下载）
	s.mux.HandleFunc("/local_merge", s.withAuth(s.localMergeHandler))

	// 健康检查
	s.mux.HandleFunc("/health", s.healthHandler)
	s.mux.HandleFunc("/ready", s.readyHandler)
}

// ListenAndServe 启动 HTTP 服务
func (s *Server) ListenAndServe() error {
	addr := ":" + s.cfg.Port
	log.Printf("Flask 服务启动，监听端口: %s", s.cfg.Port)
	if s.cfg.WebUser == "" {
		log.Printf("Basic Auth 未启用")
	} else {
		log.Printf("Basic Auth 已启用，用户: %s", s.cfg.WebUser)
	}
	return http.ListenAndServe(addr, s.loggerMiddleware(s.mux))
}

// ============= 中间件 =============

// withAuth 包装需要鉴权的 handler
func (s *Server) withAuth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.WebUser != "" || s.cfg.WebPass != "" {
			user, pass, ok := r.BasicAuth()
			if !ok || user != s.cfg.WebUser || pass != s.cfg.WebPass {
				w.Header().Set("WWW-Authenticate", `Basic realm="DDM3U8"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		h(w, r)
	}
}

// loggerMiddleware 简单访问日志
func (s *Server) loggerMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		h.ServeHTTP(w, r)
	})
}

// ============= 路由 handler =============

// index 首页（返回 embed 的 templates/index.html）
func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	data, err := s.cfg.TemplatesFS.ReadFile("templates/index.html")
	if err != nil {
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}

// healthHandler /health
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyHandler /ready
func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	ffmpegOk := s.checkFFmpeg()
	dlDirOk := s.checkDownloadDir()
	ready := ffmpegOk && dlDirOk
	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]interface{}{
		"ready":          ready,
		"phase":          "running",
		"ffmpeg_ready":   ffmpegOk,
		"download_core":  "ddm3u8-go (uTLS)",
		"downloads_ready": dlDirOk,
		"db_loaded":      true,
		"errors":         []string{},
	})
}

func (s *Server) checkFFmpeg() bool {
	if s.cfg.FFmpegPath == "" {
		return false
	}
	_, err := os.Stat(s.cfg.FFmpegPath)
	if err == nil {
		return true
	}
	// 在 PATH 里找
	for _, p := range strings.Split(os.Getenv("PATH"), ":") {
		if _, err := os.Stat(p + "/" + s.cfg.FFmpegPath); err == nil {
			return true
		}
	}
	return false
}

func (s *Server) checkDownloadDir() bool {
	info, err := os.Stat(s.cfg.DownloadDir)
	return err == nil && info.IsDir()
}

// tasksHandler GET /api/tasks
func (s *Server) tasksHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, s.tm.Snapshot())
}

// taskActionHandler POST /api/task/{id} {action: cancel/delete}
//                  GET  /api/task/{id}/debug
func (s *Server) taskActionHandler(w http.ResponseWriter, r *http.Request) {
	// path 形如 /api/task/{id} 或 /api/task/{id}/debug
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	id := parts[2]

	// 子路径: /debug
	if len(parts) >= 4 && parts[3] == "debug" {
		t, ok := s.tm.Get(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "任务不存在"})
			return
		}
		writeJSON(w, http.StatusOK, t)
		return
	}

	// POST 动作
	if r.Method == http.MethodPost {
		var body struct{ Action string `json:"action"` }
		_ = json.NewDecoder(r.Body).Decode(&body)
		switch body.Action {
		case "pause": // 暂停：保留缓存，可断点恢复
			if s.tm.Pause(id) {
				w.WriteHeader(http.StatusOK)
			} else {
				http.Error(w, "cannot pause", http.StatusBadRequest)
			}
		case "cancel": // 取消：清理缓存
			if s.tm.Cancel(id) {
				w.WriteHeader(http.StatusOK)
			} else {
				http.Error(w, "cannot cancel", http.StatusBadRequest)
			}
		case "resume": // 恢复/重新执行
			if s.tm.Resume(id) {
				w.WriteHeader(http.StatusOK)
			} else {
				http.Error(w, "cannot resume", http.StatusBadRequest)
			}
		case "merge": // 强合：复用已下载的分片重新合并
			if s.tm.Merge(id) {
				w.WriteHeader(http.StatusOK)
			} else {
				http.Error(w, "cannot merge", http.StatusBadRequest)
			}
		case "delete":
			if s.tm.Delete(id) {
				w.WriteHeader(http.StatusOK)
			} else {
				http.Error(w, "cannot delete", http.StatusBadRequest)
			}
		default:
			http.Error(w, "unknown action", http.StatusBadRequest)
		}
		return
	}
}

// clearHandler POST /api/clear 清除所有非活跃任务
func (s *Server) clearHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	n := s.tm.Clear()
	w.WriteHeader(http.StatusOK)
	_ = n
}

// clearSelectedHandler POST /api/clear-selected {ids:[]}
func (s *Server) clearSelectedHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct{ IDs []string `json:"ids"` }
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	deleted := 0
	for _, id := range body.IDs {
		if s.tm.Delete(id) {
			deleted++
		}
	}
	writeJSON(w, http.StatusOK, map[string]int{"deleted": deleted})
}

// foldersHandler GET /api/folders
func (s *Server) foldersHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"folders": s.tm.ListFolders(),
	})
}

// videoFilesHandler GET /api/video_files
func (s *Server) videoFilesHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"videos": s.tm.ListVideoFiles(),
	})
}

// downHandler POST /down 创建下载任务
// 接收 form-urlencoded，字段与 Flask 完全兼容：
//   url, name, referer, origin, cookie, user_agent, custom_headers, sub_path
func (s *Server) downHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "表单解析失败"})
		return
	}

	urlText := strings.TrimSpace(r.FormValue("url"))
	if urlText == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "URL不能为空"})
		return
	}

	urls := extractM3U8URLs(urlText)
	if len(urls) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "没有识别到有效的 m3u8 链接"})
		return
	}

	rawName := strings.TrimSpace(r.FormValue("name"))
	if rawName == "" {
		rawName = "video"
	}
	referer := strings.TrimSpace(r.FormValue("referer"))
	if referer == "https://" {
		referer = ""
	}
	origin := strings.TrimSpace(r.FormValue("origin"))
	cookie := sanitizeCookie(r.FormValue("cookie"))
	ua := strings.TrimSpace(r.FormValue("user_agent"))
	if ua == "" {
		ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
	}
	customHeaders := strings.TrimSpace(r.FormValue("custom_headers"))
	subPath := strings.TrimSpace(r.FormValue("sub_path"))

	// 子路径处理（防目录遍历）
	downloadDir := s.cfg.DownloadDir
	if subPath != "" {
		subPath = filepathBase(subPath)
		if subPath != "" {
			downloadDir = downloadDir + "/" + subPath
		}
	}
	_ = os.MkdirAll(downloadDir, 0755)

	// 组装 headers
	headers := map[string]string{
		"User-Agent": ua,
		"Referer":    referer,
		"Origin":     origin,
		"Cookie":     cookie,
	}
	// 自定义头（每行 Key:Value）
	for _, line := range strings.Split(customHeaders, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		idx := strings.Index(line, ":")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		headers[key] = val
	}

	// 创建任务
	created := 0
	// 并发数（前端传入，默认3，范围1-10；非限制资源可手动调高）
	concurrency := 3
	if c := strings.TrimSpace(r.FormValue("concurrency")); c != "" {
		if n, err := strconv.Atoi(c); err == nil && n >= 1 && n <= 10 {
			concurrency = n
		}
	}
	// TLS 指纹（前端传入，默认 chrome，可选 safari/firefox）
	fingerprint := strings.TrimSpace(r.FormValue("fingerprint"))
	if fingerprint != "" && fingerprint != "chrome" && fingerprint != "safari" && fingerprint != "firefox" {
		fingerprint = "" // 非法值置空，用默认
	}
	for i, u := range urls {
		name := rawName
		if len(urls) > 1 {
			name = fmt.Sprintf("%s_%02d", rawName, i+1)
		}
		_ = s.tm.CreateWithDir(u, name, headers, downloadDir, concurrency, fingerprint)
		created++
	}

	msg := fmt.Sprintf("已创建 %d 个任务", created)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": msg, "created": created,
	})
}

// sanitizeCookie 清洗用户粘贴的 Cookie 字符串
// 支持任意格式输入（开发者工具整段复制、Set-Cookie 散行、纯 KV 等），
// 只保留 name=value 对，丢弃 Cookie/Set-Cookie 标头前缀和 Path/Domain/Expires 等属性。
// 例：
//   "Cookie: cf_clearance=xxx; Path=/; HttpOnly; _cf_bm=yyy; Secure"
//  -> "cf_clearance=xxx; _cf_bm=yyy"
func sanitizeCookie(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	// 去掉可能的 "Cookie:" / "cookie:" / "Set-Cookie:" 前缀
	for _, p := range []string{"set-cookie:", "cookie:"} {
		if strings.HasPrefix(strings.ToLower(raw), p) {
			raw = strings.TrimSpace(raw[len(p):])
			break
		}
	}
	// 同时按 `;` 与换行切分，兼容多行粘贴
	raw = strings.ReplaceAll(raw, "\r", "\n")
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ';' || r == '\n'
	})

	// Cookie 属性 key（大小写不敏感），出现则丢弃整个 pair
	dropAttrs := map[string]struct{}{
		"path": {}, "domain": {}, "expires": {}, "max-age": {},
		"samesite": {}, "httponly": {}, "secure": {}, "comment": {},
		"version": {}, "priority": {},
	}

	type kv struct{ k, v string }
	seen := map[string]struct{}{}
	var pairs []kv
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eq := strings.Index(p, "=")
		if eq <= 0 { // 没有 '=' 或 key 为空 → 像 HttpOnly/Secure 这种 flag，丢弃
			continue
		}
		k := strings.TrimSpace(p[:eq])
		v := strings.TrimSpace(p[eq+1:])
		if k == "" || v == "" {
			continue
		}
		lk := strings.ToLower(k)
		if _, drop := dropAttrs[lk]; drop {
			continue
		}
		if _, dup := seen[k]; dup {
			continue // 同名 cookie 取第一次出现的
		}
		seen[k] = struct{}{}
		pairs = append(pairs, kv{k, v})
	}
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, p.k+"="+p.v)
	}
	return strings.Join(out, "; ")
}

// localMergeHandler POST /local_merge {folder_name}
// 扫描 /downloads/{folder_name} 下的 .ts 分片，创建一个"本地缓存合并"任务
// 复用已有分片，不重新下载。用于下载中断/失败后用残留的 .ts 缓存重新合并。
func (s *Server) localMergeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "表单解析失败"})
		return
	}
	folderName := strings.TrimSpace(r.FormValue("folder_name"))
	if folderName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请先选择缓存文件夹"})
		return
	}
	// 安全解析子路径（防目录遍历）
	folderPath, ok := safeSubPath(s.cfg.DownloadDir, folderName)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "非法的文件夹路径"})
		return
	}
	info, err := os.Stat(folderPath)
	if err != nil || !info.IsDir() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "文件夹不存在: " + folderName})
		return
	}
	// 用文件夹最后一段作为输出文件名
	id := s.tm.CreateLocalMerge(folderPath, filepathBase(folderName))
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message": "已创建本地合并任务",
		"id":      id,
	})
}

// ============= 工具函数 =============

// writeJSON 写 JSON 响应
func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// filepathBase 取路径 basename（防目录遍历）
func filepathBase(p string) string {
	p = strings.TrimSpace(p)
	p = strings.Trim(p, "/\\")
	parts := strings.Split(p, "/")
	if len(parts) == 0 {
		return ""
	}
	last := strings.TrimSpace(parts[len(parts)-1])
	if last == "." || last == ".." {
		return ""
	}
	return last
}

// safeSubPath 把用户传入的子路径解析为相对 rootDir 的安全绝对路径
// 允许多层（如 "movie1/sub"），但禁止任何 .. 逃逸和绝对路径
// 返回 (绝对路径, ok)；不安全则 ok=false
func safeSubPath(rootDir, sub string) (string, bool) {
	sub = strings.TrimSpace(sub)
	sub = strings.Trim(sub, "/\\")
	if sub == "" || sub == "." || sub == ".." {
		return "", false
	}
	// 逐段过滤，剔除 . / .. / 空段
	parts := strings.Split(sub, "/")
	clean := make([]string, 0, len(parts))
	for _, seg := range parts {
		seg = strings.TrimSpace(seg)
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		if strings.ContainsAny(seg, `<>:"|?*`) {
			return "", false
		}
		clean = append(clean, seg)
	}
	if len(clean) == 0 {
		return "", false
	}
	joined := filepath.Clean(filepath.Join(rootDir, strings.Join(clean, "/")))
	root := filepath.Clean(rootDir)
	// 必须严格位于 rootDir 之下（不允许 == root）
	if joined == root || !strings.HasPrefix(joined, root+string(filepath.Separator)) {
		return "", false
	}
	return joined, true
}

// extractM3U8URLs 从文本中提取所有 m3u8 链接
// 兼容多种粘贴格式：
//   - 每行一个 URL
//   - 空格/Tab 分隔
//   - 多个 URL 直接连在一起无分隔符（如 https://a.m3u8https://b.m3u8）
//   - URL 外带反引号、引号、括号等
//   - URL 带 query string（如 video.m3u8?token=xxx）
var m3u8URLRe = regexp.MustCompile(`https?://[^\s'"` + "`" + `\)\}\]]*?\.m3u8(?:[?#][^\s'"` + "`" + `\)\}\]]*)?`)

func extractM3U8URLs(text string) []string {
	urls := []string{}
	seen := map[string]bool{}
	for _, m := range m3u8URLRe.FindAllString(text, -1) {
		u := strings.Trim(m, "`")
		if !seen[u] {
			seen[u] = true
			urls = append(urls, u)
		}
	}
	return urls
}

// 引入 io/fs 用于 embed
var _ fs.FS = (fs.FS)(nil)

// 引入 strconv（防止编译报"imported but not used"如果某分支用不到）
var _ = strconv.Atoi
