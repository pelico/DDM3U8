import os
import subprocess
import threading
import json
import uuid
import datetime
import tarfile
import shutil
import signal
import re
import time
import logging
from flask import Flask, request, jsonify, render_template
from functools import wraps
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# ================= 日志系统配置 =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('DDM3U8')
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# ================= 全局配置 (从环境变量读取) =================
CONFIG = {
    "PORT": int(os.environ.get("PORT", 8080)),
    "DB_PATH": "/downloads/tasks_history.json",
    "BIN_PATH": "/app/N_m3u8DL-RE",
    "DOWNLOAD_DIR": "/downloads",
    "TEMP_EXTRACT_DIR": "/tmp/re_extract",
    "MAX_DOWNLOADS": int(os.environ.get("MAX_DOWNLOADS", 3))
}

def detect_download_core():
    """自动检测可用的下载核心：优先 N_m3u8DL-RE，其次 yt-dlp"""
    if os.path.exists(CONFIG["BIN_PATH"]):
        return "n_m3u8dl_re"
    if shutil.which("yt-dlp"):
        return "yt-dlp"
    return None

def curl_cffi_available():
    """检测 curl_cffi 是否可用（armv7l 无预编译 wheel，可能未安装）"""
    try:
        import curl_cffi  # noqa: F401
        return True
    except Exception:
        return False

DOWNLOAD_CORE = detect_download_core()
# curl_cffi 缺失时 yt-dlp --impersonate chrome 会启动失败，需动态决定是否加该参数
CURL_CFFI_OK = curl_cffi_available()
if DOWNLOAD_CORE == "yt-dlp" and not CURL_CFFI_OK:
    logger.warning("[环境初始化] curl_cffi 不可用，yt-dlp 将不带 --impersonate chrome，部分 CDN 可能连接被重置")

# 读取鉴权环境变量
WEB_USER = os.environ.get("WEB_USER", "").strip()
WEB_PASS = os.environ.get("WEB_PASS", "").strip()

TASK_LOCK = threading.Lock()
GLOBAL_REFERER = ""  
GLOBAL_HEADERS = {}  # 全局请求头: {"User-Agent": "...", "Referer": "...", "Origin": "...", "Cookie": "..."}
tasks = {}

# 默认浏览器 UA，避免部分 CDN 对无 UA 或爬虫 UA 直接拒绝
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
# 日志保留的最大行数（用于错误排查）
MAX_LOG_LINES = 200

app = Flask(__name__)
app.json.sort_keys = False

ACTIVE_TASK_STATUSES = {'排队中', '下载中', '合并中', '等待FFmpeg', '转换中'}

VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts', '.mpg', '.mpeg', '.rmvb', '.rm'}

_FFMPEG_CACHE = None

def is_ffmpeg_ready():
    global _FFMPEG_CACHE
    if _FFMPEG_CACHE is None:
        _FFMPEG_CACHE = shutil.which("ffmpeg") is not None
    return _FFMPEG_CACHE

def log_info(msg):
    logger.info(msg)

def log_error(msg):
    logger.error(msg)


def extract_m3u8_urls(text):
    pattern = re.compile(r'https?://(?:(?!https?://).)*?\.m3u8(?:\?(?:(?!https?://)[^\s<>"\'])*)?', re.IGNORECASE)
    urls = []
    seen = set()
    for match in pattern.finditer(text or ''):
        url = match.group(0).strip().rstrip('.,;，。；')
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls

# ================= Basic Auth 装饰器 =================
def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if WEB_USER and WEB_PASS:
            auth = request.authorization
            if not auth or auth.username != WEB_USER or auth.password != WEB_PASS:
                return jsonify({"error": "Unauthorized"}), 401, {'WWW-Authenticate': 'Basic realm="DDM3U8"'}
        return f(*args, **kwargs)
    return decorated

# ================= 配置验证 =================
def validate_config():
    logger.info("=== 配置验证 ===")
    ok = True
    
    # 验证下载目录
    if not os.path.exists(CONFIG["DOWNLOAD_DIR"]):
        try:
            os.makedirs(CONFIG["DOWNLOAD_DIR"], exist_ok=True)
            logger.info(f"创建下载目录: {CONFIG['DOWNLOAD_DIR']}")
        except Exception as e:
            log_error(f"无法创建下载目录 {CONFIG['DOWNLOAD_DIR']}: {e}")
            ok = False
    
    # 验证临时目录
    if not os.path.exists(CONFIG["TEMP_EXTRACT_DIR"]):
        try:
            os.makedirs(CONFIG["TEMP_EXTRACT_DIR"], exist_ok=True)
            logger.info(f"创建临时目录: {CONFIG['TEMP_EXTRACT_DIR']}")
        except Exception as e:
            log_error(f"无法创建临时目录 {CONFIG['TEMP_EXTRACT_DIR']}: {e}")
            ok = False
    
    # 验证 ffmpeg - 不强制要求，允许后台安装
    if not shutil.which("ffmpeg"):
        logger.warning("⚠️ ffmpeg 暂未安装，服务将继续运行，等待后台安装完成后才能进行合并操作")
    else:
        logger.info("ffmpeg 已安装")
    
    # 验证下载核心
    global DOWNLOAD_CORE
    DOWNLOAD_CORE = detect_download_core()
    if DOWNLOAD_CORE == "n_m3u8dl_re":
        logger.info(f"下载核心: N_m3u8DL-RE ({CONFIG['BIN_PATH']})")
    elif DOWNLOAD_CORE == "yt-dlp":
        logger.info("下载核心: yt-dlp")
    else:
        log_error("未检测到可用的下载核心（N_m3u8DL-RE 或 yt-dlp）")
        ok = False
    
    if ok:
        logger.info("配置验证完成")
    return ok

# ================= 环境初始化 =================
def extract_and_setup(tar_path, dest_path):
    temp_extract_dir = CONFIG["TEMP_EXTRACT_DIR"]
    if os.path.exists(temp_extract_dir): shutil.rmtree(temp_extract_dir)
    os.makedirs(temp_extract_dir, exist_ok=True)
    bin_found = False
    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(path=temp_extract_dir)
            for item in os.listdir(temp_extract_dir):
                if item.startswith("N_m3u8DL-RE") and not item.endswith(".md"):
                    src_path = os.path.join(temp_extract_dir, item)
                    shutil.move(src_path, dest_path)
                    os.chmod(dest_path, 0o755)
                    bin_found = True
                    break
    finally:
        shutil.rmtree(temp_extract_dir, ignore_errors=True)
    if not bin_found: log_info("[环境初始化] 未找到N_m3u8DL-RE")

def fix_environment():
    os.makedirs(CONFIG["DOWNLOAD_DIR"], exist_ok=True)
    os.makedirs(CONFIG["TEMP_EXTRACT_DIR"], exist_ok=True)
    
    if not shutil.which("ffmpeg"):
        log_info("[环境初始化] ffmpeg 未就绪，请检查镜像构建是否已安装 ffmpeg")
    
    bin_path = CONFIG["BIN_PATH"]
    if os.path.exists(bin_path):
        try:
            os.chmod(bin_path, 0o755)
        except Exception as e:
            log_info(f"[环境初始化] 设置 N_m3u8DL-RE 权限失败: {str(e)}")
        return
    
    log_info("[环境初始化] N_m3u8DL-RE 未就绪，请检查镜像构建是否已内置对应架构二进制")

BOOT_STATE = {
    "phase": "unknown",
    "ffmpeg_ready": False,
    "bin_ready": False,
    "download_core": None,
    "db_loaded": False,
    "downloads_ready": False,
    "errors": [],
}


def refresh_boot_state():
    BOOT_STATE["ffmpeg_ready"] = shutil.which("ffmpeg") is not None
    BOOT_STATE["bin_ready"] = os.path.exists(CONFIG["BIN_PATH"])
    BOOT_STATE["download_core"] = DOWNLOAD_CORE
    BOOT_STATE["downloads_ready"] = os.path.isdir(CONFIG["DOWNLOAD_DIR"]) and os.access(CONFIG["DOWNLOAD_DIR"], os.W_OK)
    return BOOT_STATE


def save_tasks():
    with TASK_LOCK:
        tmp_path = CONFIG["DB_PATH"] + ".tmp"
        try:
            serializable_tasks = {tid: {k: v for k, v in t.items() if k != 'process'} for tid, t in tasks.items()}
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_tasks, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, CONFIG["DB_PATH"])
        except Exception as e:
            log_info(f"[持久化失败] {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


def _backup_corrupt_db(path):
    try:
        if os.path.exists(path):
            bak = path + ".corrupt"
            shutil.copy2(path, bak)
    except Exception as e:
        log_info(f"[持久化] 备份损坏数据库失败: {e}")


def load_tasks():
    global tasks
    db_path = CONFIG["DB_PATH"]
    tasks = {}
    if not os.path.exists(db_path):
        BOOT_STATE["db_loaded"] = True
        return
    try:
        with open(db_path, 'r', encoding='utf-8') as f:
            loaded = json.load(f)
        for tid, t in loaded.items():
            if t.get('status') in ['下载中', '排队中', '合并中', '转换中']:
                t['status'] = '已中断'
                t['log'] = '系统重启导致中断，可点击恢复'
                t['process'] = None
        tasks = loaded
        BOOT_STATE["db_loaded"] = True
    except Exception as e:
        BOOT_STATE["errors"].append(f"task_db_load_failed:{e}")
        _backup_corrupt_db(db_path)
        tasks = {}

def cleanup_orphan_temp_dirs():
    """清理无主的临时目录（容器重启后的残留文件）"""
    try:
        download_dir = CONFIG["DOWNLOAD_DIR"]
        if not os.path.exists(download_dir):
            return
        active_temp_dirs = set()
        for t in tasks.values():
            td = t.get('temp_dir')
            if td:
                active_temp_dirs.add(os.path.basename(td))
        cleaned = 0
        for item in os.listdir(download_dir):
            if item.endswith('_temp') and item not in active_temp_dirs:
                temp_path = os.path.join(download_dir, item)
                if os.path.isdir(temp_path):
                    shutil.rmtree(temp_path, ignore_errors=True)
                    cleaned += 1
        if cleaned > 0:
            log_info(f"[启动清理] 清理了 {cleaned} 个残留临时目录")
    except Exception as e:
        log_error(f"[启动清理] 扫描残留目录失败: {e}")


def run_environment_setup():
    BOOT_STATE["phase"] = "environment"
    logger.info("[启动] 开始环境初始化")
    try:
        fix_environment()
    except Exception as e:
        BOOT_STATE["errors"].append(f"environment_setup_failed:{e}")
        logger.error(f"[启动] 环境初始化阶段异常: {e}")


def run_startup_validation():
    BOOT_STATE["phase"] = "validation"
    logger.info("[启动] 开始配置校验")
    try:
        if not validate_config():
            BOOT_STATE["errors"].append("startup_validation_warning")
    except Exception as e:
        BOOT_STATE["errors"].append(f"validate_config_failed:{e}")
        logger.error(f"[启动] 配置校验阶段异常: {e}")


def boot():
    run_environment_setup()
    run_startup_validation()
    load_tasks()
    cleanup_orphan_temp_dirs()
    refresh_boot_state()
    BOOT_STATE["phase"] = "ready"
    if BOOT_STATE["errors"]:
        logger.warning(f"[启动] 完成，但存在降级项: {BOOT_STATE['errors']}")
    else:
        logger.info("[启动] 初始化完成")

# ================= 后端业务逻辑 =================
def execute_merge_logic(task_id, target_tmp_dir, final_out_file, log_title):
    try:
        if not os.path.exists(target_tmp_dir): raise Exception("未找到缓存目录")

        target_sub_dir, m3u8_file_path = None, None
        for root, dirs, files in os.walk(target_tmp_dir):
            # yt-dlp 分片可能是 .part-Frag，N_m3u8DL-RE 是 .ts/.m4s，伪装分片可能是 .jpeg
            if any(f.endswith(('.ts', '.m4s', '.jpeg')) or '.part-Frag' in f for f in files):
                target_sub_dir = root
                for m_root, m_dirs, m_files in os.walk(target_tmp_dir):
                     for f in m_files:
                        if f.endswith('.m3u8'):
                            m3u8_file_path = os.path.join(m_root, f)
                            break
                     if m3u8_file_path: break
                break

        if not target_sub_dir: raise Exception("未找到任何有效的视频碎片目录")
        
        ts_files_in_order = []
        if m3u8_file_path:
            log_info(f"[{log_title}] 找到清单文件 {os.path.basename(m3u8_file_path)}，将按其真实顺序合并")
            with open(m3u8_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if os.path.exists(os.path.join(target_sub_dir, line)):
                            ts_files_in_order.append(line)
        
        if not ts_files_in_order:
            log_info(f"[{log_title}] 未找到有效的m3u8清单，将尝试按文件名自然排序（可能导致乱序）")
            ts_files = [f for f in os.listdir(target_sub_dir) if f.endswith(('.ts', '.m4s', '.jpeg')) or '.part-Frag' in f]
            def natural_keys(text): return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', text)]
            ts_files.sort(key=natural_keys)
            ts_files_in_order = ts_files

        if not ts_files_in_order: raise Exception("碎片文件列表为空，无法合并")

        log_info(f"[{log_title}] 共有 {len(ts_files_in_order)} 个有效碎片准备合成...")
        
        list_path = os.path.join(target_sub_dir, "input.txt")
        with open(list_path, "w", encoding="utf-8") as f:
            for ts in ts_files_in_order: f.write(f"file '{ts}'\n")
        
        merge_cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", "input.txt", "-c", "copy", final_out_file]
        process = subprocess.Popen(merge_cmd, cwd=target_sub_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='ignore')
        
        for line in iter(process.stdout.readline, ''):
            if "time=" in line or "fps=" in line:
                with TASK_LOCK:
                    if task_id in tasks: tasks[task_id]['log'] = line.strip()[-100:]
        
        process.wait()

        with TASK_LOCK:
            if task_id in tasks:
                if process.returncode == 0 and os.path.exists(final_out_file):
                    tasks[task_id]['status'] = '完成(强合)'
                    tasks[task_id]['log'] = f'⚠️ 成功合并 {len(ts_files_in_order)} 个碎片'
                    shutil.rmtree(target_tmp_dir, ignore_errors=True)
                else:
                    tasks[task_id]['status'] = '错误'
                    tasks[task_id]['log'] = f'❌ FFmpeg拒绝合并(退出码:{process.returncode})'
    except Exception as e:
        with TASK_LOCK:
            if task_id in tasks:
                tasks[task_id]['status'] = '错误'
                tasks[task_id]['log'] = f'强合失败: {str(e)[:60]}'
    finally:
        save_tasks()

def run_manual_merge(task_id, task_name):
    with TASK_LOCK:
        if task_id not in tasks: return
        tasks[task_id]['status'] = '合并中'
        download_dir = tasks[task_id].get('download_dir', CONFIG["DOWNLOAD_DIR"])
    save_tasks()
    tmp_dir = os.path.join(download_dir, f"{task_name}_temp")
    out_file = os.path.join(download_dir, f"{task_name}.mp4")
    execute_merge_logic(task_id, tmp_dir, out_file, "手动强合")

def run_local_merge_tool(task_id, folder_name):
    with TASK_LOCK:
        if task_id not in tasks: return
        tasks[task_id]['status'] = '合并中'
    save_tasks()
    safe_folder = os.path.basename(folder_name.strip('/\\'))
    tmp_dir = os.path.join(CONFIG["DOWNLOAD_DIR"], safe_folder)
    out_file = os.path.join(CONFIG["DOWNLOAD_DIR"], f"{safe_folder}_merged.mp4")
    execute_merge_logic(task_id, tmp_dir, out_file, "工具强合")

def run_audio_extract(task_id, audio_target):
    """audio_target: dict with keys: input_path, output_path"""
    input_path = audio_target['input_path']
    output_path = audio_target['output_path']
    task_name = tasks.get(task_id, {}).get('name', 'Unknown')
    log_info(f"[音频提取] 任务 [{task_name}] 开始: {os.path.basename(input_path)}")
    try:
        with TASK_LOCK:
            if task_id in tasks:
                tasks[task_id]['status'] = '转换中'
                tasks[task_id]['process'] = None
        save_tasks()

        cmd = ["ffmpeg", "-y", "-i", input_path, "-vn", "-acodec", "aac", "-b:a", "192k", output_path]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='ignore')

        with TASK_LOCK:
            if task_id in tasks:
                tasks[task_id]['process'] = process

        for line in iter(process.stdout.readline, ''):
            with TASK_LOCK:
                if task_id not in tasks:
                    break
                if tasks[task_id].get('status') != '转换中':
                    break
                if "time=" in line or "size=" in line:
                    tasks[task_id]['log'] = line.strip()[-100:]

        process.wait()

        with TASK_LOCK:
            if task_id in tasks:
                if process.returncode == 0 and os.path.exists(output_path):
                    tasks[task_id]['status'] = '完成(音频)'
                    tasks[task_id]['log'] = f'✅ 音频提取成功: {os.path.basename(output_path)}'
                else:
                    tasks[task_id]['status'] = '错误'
                    tasks[task_id]['log'] = f'❌ FFmpeg转换失败(退出码:{process.returncode})'
    except Exception as e:
        log_error(f"[音频提取] 任务 [{task_name}] 异常: {e}")
        with TASK_LOCK:
            if task_id in tasks:
                tasks[task_id]['status'] = '错误'
                tasks[task_id]['log'] = str(e)[:100]
    finally:
        with TASK_LOCK:
            if task_id in tasks:
                tasks[task_id]['process'] = None
        if 'process' in locals() and process.stdout:
            process.stdout.close()
        save_tasks()

def run_download(task_id, cmd):
    task_name = tasks.get(task_id, {}).get('name', 'Unknown')
    core = tasks.get(task_id, {}).get('core', 'n_m3u8dl_re')
    log_info(f"[调度器] 任务 [{task_name}] 开始执行 (core={core})")
    log_info(f"[调度器] 命令: {' '.join(cmd)}")
    # 用于保存全部输出（最后 MAX_LOG_LINES 行），出错时回显给用户定位问题
    recent_lines = []
    try:
        with TASK_LOCK: 
            tasks[task_id]['status'] = '下载中'
            tasks[task_id]['process'] = None
        save_tasks()
        
        # yt-dlp 需要将分片缓存落到 temp_dir（通过 TMPDIR + cwd 控制）
        # N_m3u8DL-RE 由 --tmp-dir 参数自行控制，这里保持默认
        popen_kwargs = dict(stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, encoding='utf-8', errors='ignore')
        if core == 'yt-dlp':
            temp_dir = tasks.get(task_id, {}).get('temp_dir', CONFIG["DOWNLOAD_DIR"])
            env = os.environ.copy()
            env['TMPDIR'] = temp_dir
            popen_kwargs['env'] = env
            popen_kwargs['cwd'] = temp_dir

        process = subprocess.Popen(cmd, **popen_kwargs)
        
        with TASK_LOCK:
            tasks[task_id]['process'] = process
        
        # 读取输出：保留全部行用于排错，进度行实时更新到 log
        for line in iter(process.stdout.readline, ''):
            with TASK_LOCK:
                if task_id not in tasks:
                    break
                current_status = tasks[task_id].get('status')
                if current_status != '下载中':
                    log_info(f"[调度器] 任务 [{task_name}] 状态已变更为 {current_status}，停止读取输出")
                    break
            log_content = line.strip()
            recent_lines.append(log_content)
            if len(recent_lines) > MAX_LOG_LINES:
                recent_lines.pop(0)
            # yt-dlp 进度行含 "downloading"，N_m3u8DL-RE 含 % / B/s
            if "%" in log_content or "B/s" in log_content or "downloading" in log_content.lower(): 
                with TASK_LOCK:
                    if task_id in tasks:
                        tasks[task_id]['log'] = log_content[-100:]
        
        process.wait()
        
        with TASK_LOCK:
            if task_id not in tasks or tasks[task_id]['status'] != '下载中':
                pass
            elif core == 'yt-dlp':
                # yt-dlp 输出 .ts，需 ffmpeg 封装为 .mp4
                download_dir = tasks[task_id].get('download_dir', CONFIG["DOWNLOAD_DIR"])
                temp_dir = tasks[task_id].get('temp_dir', os.path.join(download_dir, f"{task_name}_temp"))
                temp_ts = os.path.join(temp_dir, f"{task_name}.ts")
                if process.returncode == 0 and os.path.exists(temp_ts):
                    tasks[task_id]['status'] = '合并中'
                    tasks[task_id]['log'] = '正在封装为MP4...'
                else:
                    err_tail = ' | '.join(l for l in recent_lines[-30:] if l)
                    reason = f"进程退出码 {process.returncode}" if process.returncode != 0 else "假成功(未生成TS文件)"
                    tasks[task_id]['status'] = '错误'
                    tasks[task_id]['log'] = f'❌ {reason} 末尾输出: {err_tail[:2000]}'
                    log_error(f"[调度器] 任务 [{task_name}] 失败: {reason}; 末尾输出: {err_tail}")
            else:
                # N_m3u8DL-RE 直接输出 .mp4
                download_dir = tasks[task_id].get('download_dir', CONFIG["DOWNLOAD_DIR"])
                expected_out_file = os.path.join(download_dir, f"{task_name}.mp4")
                if process.returncode == 0 and os.path.exists(expected_out_file):
                    tasks[task_id]['status'] = '已完成'
                    tasks[task_id]['log'] = '✅ 完整下载并合并成功'
                else:
                    err_tail = ' | '.join(l for l in recent_lines[-30:] if l)
                    reason = f"进程退出码 {process.returncode}" if process.returncode != 0 else "假成功(未生成最终MP4)"
                    tasks[task_id]['status'] = '错误'
                    tasks[task_id]['log'] = f'❌ {reason} 末尾输出: {err_tail[:2000]}'
                    log_error(f"[调度器] 任务 [{task_name}] 失败: {reason}; 末尾输出: {err_tail}")

        # yt-dlp 下载完成后，ffmpeg 将 .ts 封装为 .mp4（流复制，不重编码）
        if tasks.get(task_id, {}).get('status') == '合并中':
            try:
                download_dir = tasks[task_id].get('download_dir', CONFIG["DOWNLOAD_DIR"])
                temp_dir = tasks[task_id].get('temp_dir', os.path.join(download_dir, f"{task_name}_temp"))
                temp_ts = os.path.join(temp_dir, f"{task_name}.ts")
                final_mp4 = os.path.join(download_dir, f"{task_name}.mp4")
                ffmpeg_cmd = ["ffmpeg", "-y", "-i", temp_ts, "-c", "copy", "-bsf:a", "aac_adtstoasc", "-movflags", "+faststart", final_mp4]
                ffmpeg_proc = subprocess.run(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='ignore')
                if ffmpeg_proc.returncode == 0 and os.path.exists(final_mp4):
                    os.remove(temp_ts)
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    with TASK_LOCK:
                        if task_id in tasks:
                            tasks[task_id]['status'] = '已完成'
                            tasks[task_id]['log'] = '✅ 完整下载并合并成功'
                else:
                    raise Exception(f"ffmpeg封装失败: returncode={ffmpeg_proc.returncode}")
            except Exception as e:
                log_error(f"[调度器] ffmpeg封装失败: {e}")
                with TASK_LOCK:
                    if task_id in tasks:
                        tasks[task_id]['status'] = '错误'
                        tasks[task_id]['log'] = f'封装失败: {str(e)[:60]}，可点[强合]重试'
    except Exception as e:
        log_error(f"[调度器] 任务 [{task_name}] 执行异常: {e}")
        with TASK_LOCK:
            if task_id in tasks:
                tasks[task_id]['status'] = '错误'
                tasks[task_id]['log'] = f'❌ 异常: {str(e)[:120]}'
    finally:
        with TASK_LOCK:
            if task_id in tasks: 
                tasks[task_id]['process'] = None
        if 'process' in locals() and process.stdout: 
            process.stdout.close()
        save_tasks()

# ================= Flask 路由 =================
@app.route('/health')
def health():
    return jsonify({"status": "ok"}), 200


@app.route('/ready')
def ready():
    state = refresh_boot_state()
    # 下载核心就绪判断：N_m3u8DL-RE 存在 或 yt-dlp 可用
    core_ready = state["bin_ready"] or state["download_core"] == "yt-dlp"
    ready_ok = state["downloads_ready"] and core_ready and state["ffmpeg_ready"]
    status_code = 200 if ready_ok else 503
    return jsonify({
        "ready": ready_ok,
        "phase": state["phase"],
        "ffmpeg_ready": state["ffmpeg_ready"],
        "bin_ready": state["bin_ready"],
        "download_core": state["download_core"],
        "downloads_ready": state["downloads_ready"],
        "db_loaded": state["db_loaded"],
        "errors": state["errors"][-5:],
    }), status_code


@app.route('/')
@requires_auth
def index():
    return render_template('index.html')

@app.route('/api/tasks')
@requires_auth
def get_tasks():
    with TASK_LOCK:
        active_workers = sum(1 for t in tasks.values() if t['status'] in ['下载中', '合并中', '转换中'])
        ordered_items = sorted(
            tasks.items(),
            key=lambda item: item[1].get('created_at', ''),
            reverse=True
        )
        data = {
            "tasks": {tid: {k: v for k, v in t.items() if k != 'process'} for tid, t in ordered_items},
            "task_order": [tid for tid, _ in ordered_items],
            "active_workers": active_workers,
            "max_workers": CONFIG["MAX_DOWNLOADS"]
        }
    return jsonify(data)

@app.route('/api/task/<task_id>/debug')
@requires_auth
def debug_task(task_id):
    """调试接口：返回任务的完整命令、请求头和状态，便于排查下载失败原因"""
    with TASK_LOCK:
        task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "任务不存在"}), 404
    # 脱敏：Cookie 只显示前 20 字符
    safe_headers = dict(task.get('headers', {}))
    if safe_headers.get('Cookie'):
        c = safe_headers['Cookie']
        safe_headers['Cookie'] = c[:20] + '...(已脱敏)' if len(c) > 20 else c
    return jsonify({
        "id": task_id,
        "name": task.get('name'),
        "url": task.get('url'),
        "status": task.get('status'),
        "log": task.get('log'),
        "cmd": task.get('cmd'),
        "cmd_str": ' '.join(task.get('cmd', [])),
        "headers": safe_headers,
        "download_dir": task.get('download_dir'),
        "temp_dir": task.get('temp_dir'),
    })

@app.route('/api/folders')
@requires_auth
def get_folders():
    try:
        base_dir = CONFIG["DOWNLOAD_DIR"]
        folders = []
        # 扫描根目录
        for item in os.listdir(base_dir):
            item_path = os.path.join(base_dir, item)
            if os.path.isdir(item_path) and not item.startswith('.'):
                folders.append(item)
                # 扫描一层子目录
                try:
                    for sub_item in os.listdir(item_path):
                        sub_path = os.path.join(item_path, sub_item)
                        if os.path.isdir(sub_path) and not sub_item.startswith('.'):
                            folders.append(f"{item}/{sub_item}")
                except:
                    pass
        return jsonify({"folders": sorted(folders)})
    except Exception as e:
        log_error(f"获取文件夹列表失败: {e}")
        return jsonify({"folders": []})

@app.route('/api/video_files')
@requires_auth
def get_video_files():
    try:
        base_dir = CONFIG["DOWNLOAD_DIR"]
        videos = []
        for root, dirs, files in os.walk(base_dir):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext in VIDEO_EXTENSIONS:
                    full_path = os.path.join(root, f)
                    try:
                        size = os.path.getsize(full_path)
                    except:
                        size = 0
                    rel_path = os.path.relpath(full_path, base_dir)
                    videos.append({
                        "name": f,
                        "path": rel_path.replace('\\', '/'),
                        "size": size
                    })
        videos.sort(key=lambda v: v['name'].lower())
        return jsonify({"videos": videos})
    except Exception as e:
        log_error(f"获取视频文件列表失败: {e}")
        return jsonify({"videos": []})

@app.route('/api/clear', methods=['POST'])
@requires_auth
def clear_tasks():
    with TASK_LOCK:
        to_delete = [tid for tid, t in tasks.items() if t['status'] not in ['下载中', '排队中', '合并中', '转换中']]
        for tid in to_delete: tasks.pop(tid, None)
    save_tasks()
    return '', 200

@app.route('/api/clear-selected', methods=['POST'])
@requires_auth
def clear_selected_tasks():
    try:
        data = request.get_json() or {}
        ids = data.get('ids', [])
    except Exception as e:
        log_error(f"解析请求体失败: {e}")
        return '', 400
    with TASK_LOCK:
        to_delete = [tid for tid in ids if tid in tasks and tasks[tid]['status'] not in ['下载中', '排队中', '合并中', '转换中']]
        for tid in to_delete: tasks.pop(tid, None)
    save_tasks()
    return jsonify({"deleted": len(to_delete)}), 200

@app.route('/api/task/<task_id>', methods=['POST'])
@requires_auth
def manage_task(task_id):
    with TASK_LOCK:
        if task_id not in tasks:
            return '', 200
        
        try:
            action = request.get_json().get('action')
        except Exception as e:
            log_error(f"解析请求体失败: {e}")
            return '', 400
        
        task = tasks[task_id]
        process = task.get('process')
        
        if action == 'pause' and process:
            log_info(f"[任务管理] 暂停任务: {task_id}")
            try:
                process.terminate()
                try: 
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired: 
                    log_info(f"[任务管理] 强制终止任务: {task_id}")
                    process.kill()
                task['status'] = '已暂停'
                task['log'] = '已暂停，保留缓存可恢复'
            except Exception as e:
                log_error(f"[任务管理] 暂停任务失败: {e}")
        
        elif action == 'resume' and (task.get('cmd') or task.get('audio_target')):
            log_info(f"[任务管理] 恢复任务: {task_id}")
            task['status'] = '排队中'
            task['log'] = '正在重新排队...'
        
        elif action == 'merge':
            if task['status'] not in ['下载中', '合并中']:
                log_info(f"[任务管理] 强制合并任务: {task_id}")
                if task.get('cmd'):
                    threading.Thread(target=run_manual_merge, args=(task_id, task['name'])).start()
                else:
                    threading.Thread(target=run_local_merge_tool, args=(task_id, task['folder_target'])).start()
        
        elif action == 'cancel':
            log_info(f"[任务管理] 取消任务: {task_id}")
            if process:
                try:
                    process.terminate()
                    try: 
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired: 
                        log_info(f"[任务管理] 强制终止任务: {task_id}")
                        process.kill()
                except Exception as e:
                    log_error(f"[任务管理] 终止进程失败: {e}")
            task['status'] = '已取消'
            task['log'] = '任务已取消'
            temp_dir = task.get('temp_dir')
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
    
    save_tasks()
    return '', 200

@app.route('/down', methods=['POST'])
@requires_auth
def down():
    global GLOBAL_HEADERS
    try:
        url_text = request.form.get('url', '').strip()
        raw_name = request.form.get('name', 'video').strip()
        referer_val = request.form.get('referer', '').strip()
        sub_path = request.form.get('sub_path', '').strip()
        # 下载核心选择
        core_val = request.form.get('core', '').strip()
        # 新增：更多请求头
        user_agent_val = request.form.get('user_agent', '').strip()
        origin_val = request.form.get('origin', '').strip()
        cookie_val = request.form.get('cookie', '').strip()
        custom_headers_val = request.form.get('custom_headers', '').strip()
        
        if not url_text:
            return jsonify({"error": "URL不能为空"}), 400

        urls = extract_m3u8_urls(url_text)
        if not urls:
            return jsonify({"error": "没有识别到有效的 m3u8 链接"}), 400
        
        if referer_val == "https://" or not referer_val: 
            referer_val = ""
        
        # 组装请求头字典（每次提交都重新生成，避免串任务）
        headers = {
            "User-Agent": user_agent_val or DEFAULT_USER_AGENT,
            "Referer": referer_val,
            "Origin": origin_val,
            "Cookie": cookie_val,
            "Custom": custom_headers_val,
        }
        with TASK_LOCK:
            GLOBAL_HEADERS = headers
        
        # 处理下载子目录
        download_dir = CONFIG["DOWNLOAD_DIR"]
        if sub_path:
            # 清理路径，防止目录遍历
            sub_path = os.path.basename(sub_path.strip('/\\'))
            if sub_path:
                download_dir = os.path.join(download_dir, sub_path)
        # 始终确保下载目录存在（不只在 sub_path 非空时创建）：
        # boot() 的 fix_environment 已建 /downloads，但容器卷挂载变化、
        # 手动清理 /downloads、或 sub_path 为空时旧逻辑跳过 makedirs，
        # 都会让 N_m3u8DL-RE 写入时报 errno 2 (ENOENT)。这里兜底重建。
        os.makedirs(download_dir, exist_ok=True)
        
        with TASK_LOCK:
            active_urls = {t.get('url'): t for t in tasks.values() if t.get('status') in ACTIVE_TASK_STATUSES}

        created_count = 0
        skipped_names = []
        total_count = len(urls)
        timestamp = datetime.datetime.now().strftime('%m%d_%H%M%S')

        for index, url in enumerate(urls, 1):
            existing_task = active_urls.get(url)
            if existing_task:
                skipped_names.append(existing_task.get('name', '未命名任务'))
                continue

            task_id = str(uuid.uuid4())[:8]
            if total_count > 1:
                name = f"{raw_name}_{index:02d}_{timestamp}_{task_id[:3]}"
            else:
                name = f"{raw_name}_{timestamp}_{task_id[:3]}"

            log_info(f"[任务创建] 新下载任务: {task_id} - {name} -> {download_dir}")
            start_task(url, name, task_id, download_dir, headers=headers, core=core_val)
            active_urls[url] = {"name": name, "status": "排队中"}
            created_count += 1

        if created_count == 0:
            return jsonify({"error": f"链接均已存在活跃任务：{', '.join(skipped_names[:3])}"}), 409

        message = f"已创建 {created_count} 个任务"
        if skipped_names:
            message += f"，跳过 {len(skipped_names)} 个重复任务"
        return jsonify({"message": message, "created": created_count, "skipped": len(skipped_names)}), 200
    except Exception as e:
        log_error(f"创建下载任务失败: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/local_merge', methods=['POST'])
@requires_auth
def local_merge():
    try:
        folder_name = request.form.get('folder_name', '').strip()
        if not folder_name:
            return jsonify({"error": "文件夹名称不能为空"}), 400
        
        task_id = str(uuid.uuid4())[:8]
        final_out_name = f"{folder_name}_merged"
        
        with TASK_LOCK:
            tasks[task_id] = {
                'url': f'本地文件夹: {folder_name}', 
                'name': final_out_name, 
                'cmd': None, 
                'status': '排队中', 
                'log': '等待执行扫描...', 
                'folder_target': folder_name, 
                'created_at': datetime.datetime.now().isoformat(timespec='seconds'),
                'process': None
            }
        
        save_tasks()
        log_info(f"[任务创建] 新本地合并任务: {task_id} - {folder_name}")
        return '', 200
    except Exception as e:
        log_error(f"创建本地合并任务失败: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/audio_extract', methods=['POST'])
@requires_auth
def audio_extract():
    try:
        data = request.get_json() or {}
        files = data.get('files', [])
        if not files:
            return jsonify({"error": "未选择任何视频文件"}), 400

        created = 0
        for f in files:
            rel_path = f.get('path', '').strip()
            if not rel_path:
                continue
            safe_path = os.path.normpath(rel_path).lstrip(os.sep)
            if safe_path.startswith('..'):
                continue
            input_path = os.path.join(CONFIG["DOWNLOAD_DIR"], safe_path)
            if not os.path.exists(input_path):
                continue

            base_name = os.path.splitext(os.path.basename(safe_path))[0]
            parent_dir = os.path.dirname(safe_path)
            output_name = f"{base_name}.m4a"
            output_path = os.path.join(CONFIG["DOWNLOAD_DIR"], parent_dir, output_name) if parent_dir else os.path.join(CONFIG["DOWNLOAD_DIR"], output_name)

            task_id = str(uuid.uuid4())[:8]
            timestamp = datetime.datetime.now().strftime('%m%d_%H%M%S')
            task_name = f"{base_name}_{timestamp}_{task_id[:3]}"

            with TASK_LOCK:
                tasks[task_id] = {
                    'url': f'音频提取: {os.path.basename(input_path)}',
                    'name': task_name,
                    'cmd': None,
                    'status': '排队中',
                    'log': '等待转换...',
                    'audio_target': {'input_path': input_path, 'output_path': output_path},
                    'created_at': datetime.datetime.now().isoformat(timespec='seconds'),
                    'process': None,
                    'download_dir': os.path.dirname(input_path)
                }
            created += 1

        if created == 0:
            return jsonify({"error": "没有有效的视频文件可转换"}), 400

        save_tasks()
        log_info(f"[音频提取] 创建了 {created} 个转换任务")
        return jsonify({"message": f"已创建 {created} 个音频提取任务", "created": created}), 200
    except Exception as e:
        log_error(f"创建音频提取任务失败: {e}")
        return jsonify({"error": str(e)}), 500

def start_task(url, name, task_id, download_dir=None, headers=None, core=None):
    """
    headers: dict, 可选的请求头
    core: "n_m3u8dl_re" 或 "yt-dlp"，不传则自动检测
    """
    if download_dir is None:
        download_dir = CONFIG["DOWNLOAD_DIR"]
    if headers is None:
        headers = {}

    ua = headers.get('User-Agent') or DEFAULT_USER_AGENT
    # 优先使用调用方指定的 core，否则用自动检测的
    if core not in ("n_m3u8dl_re", "yt-dlp"):
        core = DOWNLOAD_CORE or "n_m3u8dl_re"

    if core == "yt-dlp":
        # yt-dlp 两步走方案（对齐 armv7l 分支）：
        #   1) yt-dlp 下载原始 .ts 到 <temp_dir>/<name>.ts（--hls-prefer-native --fixup never 不做封装）
        #   2) run_download 里用 ffmpeg -c copy 封装为 <download_dir>/<name>.mp4
        # 分片缓存落到 temp_dir（通过 TMPDIR/cwd 控制），中断后 temp_dir 含 .ts 可强合
        temp_ts = os.path.join(download_dir, f"{name}_temp", f"{name}.ts")
        cmd = [
            "yt-dlp", url,
            "-o", temp_ts,
            "--concurrent-fragments", "10",
            "--hls-prefer-native",
            "--no-part",
            "--no-mtime",
            "--fixup", "never",
            "--retries", "10",
            "--fragment-retries", "10",
            "--retry-sleep", "fragment:exp=1:60",
            "--user-agent", ua,
        ]
        # curl_cffi 缺失时（如 armv7l 无预编译 wheel）跳过，避免 yt-dlp 启动报错
        if CURL_CFFI_OK:
            cmd.extend(["--impersonate", "chrome"])
        if headers.get("Referer"):
            cmd.extend(["--add-header", f"Referer:{headers['Referer']}"])
        if headers.get("Origin"):
            cmd.extend(["--add-header", f"Origin:{headers['Origin']}"])
        if headers.get("Cookie"):
            cmd.extend(["--add-header", f"Cookie:{headers['Cookie']}"])
        for custom in headers.get("Custom", "").splitlines():
            custom = custom.strip()
            if custom and ":" in custom:
                cmd.extend(["--add-header", custom])
    else:
        # N_m3u8DL-RE 命令
        cmd = [
            CONFIG["BIN_PATH"], url,
            "--save-name", name,
            "--save-dir", download_dir,
            "--tmp-dir", download_dir,
            "-M", "format=mp4",
            "--thread-count", "10",
            "--header", f"User-Agent:{ua}",
        ]
        if headers.get("Referer"):
            cmd.extend(["--header", f"Referer:{headers['Referer']}"])
        if headers.get("Origin"):
            cmd.extend(["--header", f"Origin:{headers['Origin']}"])
        if headers.get("Cookie"):
            cmd.extend(["--header", f"Cookie:{headers['Cookie']}"])
        for custom in headers.get("Custom", "").splitlines():
            custom = custom.strip()
            if custom and ":" in custom:
                cmd.extend(["--header", custom])

    temp_dir = os.path.join(download_dir, f"{name}_temp")
    # 两种核心都需要 temp_dir：
    #   N_m3u8DL-RE: 分片缓存目录
    #   yt-dlp: 输出 .ts 文件 + 分片缓存（TMPDIR/cwd 指向此处）
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(download_dir, exist_ok=True)
    with TASK_LOCK:
        tasks[task_id] = {
            'url': url, 
            'name': name, 
            'cmd': cmd, 
            'status': '排队中', 
            'log': '准备中...', 
            'created_at': datetime.datetime.now().isoformat(timespec='seconds'),
            'process': None,
            'download_dir': download_dir,
            'temp_dir': temp_dir,
            'headers': headers,
            'core': core
        }
    save_tasks()

def scheduler_loop():
    while True:
        try:
            with TASK_LOCK:
                ffmpeg_ready = is_ffmpeg_ready()
                
                if ffmpeg_ready:
                    for tid, t in tasks.items():
                        if t['status'] == '等待FFmpeg':
                            t['status'] = '排队中'
                            t['log'] = 'FFmpeg就绪，开始排队...'
                
                active_tasks = sum(1 for t in tasks.values() if t['status'] in ['下载中', '合并中', '转换中'])
                if active_tasks < CONFIG["MAX_DOWNLOADS"]:
                    task_to_run = next(((tid, t) for tid, t in tasks.items() if t['status'] == '排队中'), None)
                    if task_to_run:
                        tid, task = task_to_run
                        if task.get('cmd'):
                            if ffmpeg_ready:
                                threading.Thread(target=run_download, args=(tid, task['cmd'])).start()
                            else:
                                task['status'] = '等待FFmpeg'
                                task['log'] = '等待FFmpeg安装完成...'
                        elif task.get('audio_target'):
                            if ffmpeg_ready:
                                threading.Thread(target=run_audio_extract, args=(tid, task['audio_target'])).start()
                            else:
                                task['status'] = '等待FFmpeg'
                                task['log'] = '等待FFmpeg安装完成...'
                        else:
                            if ffmpeg_ready:
                                threading.Thread(target=run_local_merge_tool, args=(tid, task['folder_target'])).start()
                            else:
                                task['status'] = '等待FFmpeg'
                                task['log'] = '等待FFmpeg安装完成...'
        except Exception as e:
            log_error(f"[调度器异常] {e}")
        time.sleep(2)

if __name__ == "__main__":
    logger.info("=== DDM3U8 服务启动 ===")
    boot()
    
    # 启动调度器
    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()
    logger.info(f"调度器已启动，最大并发下载数: {CONFIG['MAX_DOWNLOADS']}")
    
    # 启动 Flask
    logger.info(f"Flask 服务启动，监听端口: {CONFIG['PORT']}")
    if WEB_USER and WEB_PASS:
        logger.info("Basic Auth 已启用")
    else:
        logger.info("Basic Auth 未启用")
    
    app.run(host='0.0.0.0', port=CONFIG["PORT"], threaded=True)
