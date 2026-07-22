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

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('DDM3U8')
logging.getLogger('werkzeug').setLevel(logging.ERROR)

CONFIG = {
    "PORT": int(os.environ.get("PORT", 8080)),
    "DB_PATH": "/downloads/tasks_history.json",
    "BIN_PATH": "yt-dlp",
    "DOWNLOAD_DIR": "/downloads",
    "TEMP_EXTRACT_DIR": "/tmp/re_extract",
    "MAX_DOWNLOADS": int(os.environ.get("MAX_DOWNLOADS", 3))
}

WEB_USER = os.environ.get("WEB_USER", "").strip()
WEB_PASS = os.environ.get("WEB_PASS", "").strip()

TASK_LOCK = threading.Lock()
GLOBAL_REFERER = ""  
tasks = {}

app = Flask(__name__)
app.json.sort_keys = False

ACTIVE_TASK_STATUSES = {'排队中', '下载中', '合并中', '等待FFmpeg'}

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

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if WEB_USER and WEB_PASS:
            auth = request.authorization
            if not auth or auth.username != WEB_USER or auth.password != WEB_PASS:
                return jsonify({"error": "Unauthorized"}), 401, {'WWW-Authenticate': 'Basic realm="DDM3U8"'}
        return f(*args, **kwargs)
    return decorated

def validate_config():
    logger.info("=== 配置验证 ===")
    ok = True
    
    if not os.path.exists(CONFIG["DOWNLOAD_DIR"]):
        try:
            os.makedirs(CONFIG["DOWNLOAD_DIR"], exist_ok=True)
            logger.info(f"创建下载目录: {CONFIG['DOWNLOAD_DIR']}")
        except Exception as e:
            log_error(f"无法创建下载目录 {CONFIG['DOWNLOAD_DIR']}: {e}")
            ok = False
    
    if not os.path.exists(CONFIG["TEMP_EXTRACT_DIR"]):
        try:
            os.makedirs(CONFIG["TEMP_EXTRACT_DIR"], exist_ok=True)
            logger.info(f"创建临时目录: {CONFIG['TEMP_EXTRACT_DIR']}")
        except Exception as e:
            log_error(f"无法创建临时目录 {CONFIG['TEMP_EXTRACT_DIR']}: {e}")
            ok = False
    
    if not shutil.which("ffmpeg"):
        logger.warning("⚠️ ffmpeg 暂未安装，服务将继续运行，等待后台安装完成后才能进行合并操作")
    else:
        logger.info("ffmpeg 已安装")
    
    if not shutil.which(CONFIG["BIN_PATH"]):
        log_error(f"{CONFIG['BIN_PATH']} 不存在")
    else:
        logger.info(f"{CONFIG['BIN_PATH']} 已就绪")
    
    if ok:
        logger.info("配置验证完成")
    return ok

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
    
    if not shutil.which(CONFIG["BIN_PATH"]):
        log_info(f"[环境初始化] {CONFIG['BIN_PATH']} 未就绪，请检查镜像构建是否已安装")

BOOT_STATE = {
    "phase": "unknown",
    "ffmpeg_ready": False,
    "bin_ready": False,
    "db_loaded": False,
    "downloads_ready": False,
    "errors": [],
}


def refresh_boot_state():
    BOOT_STATE["ffmpeg_ready"] = shutil.which("ffmpeg") is not None
    BOOT_STATE["bin_ready"] = shutil.which(CONFIG["BIN_PATH"]) is not None
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
            if t.get('status') in ['下载中', '排队中', '合并中']:
                t['status'] = '已中断'
                t['log'] = '系统重启导致中断，可点击恢复'
                t['process'] = None
        tasks = loaded
        BOOT_STATE["db_loaded"] = True
    except Exception as e:
        BOOT_STATE["errors"].append(f"task_db_load_failed:{e}")
        _backup_corrupt_db(db_path)
        tasks = {}


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
    refresh_boot_state()
    BOOT_STATE["phase"] = "ready"
    if BOOT_STATE["errors"]:
        logger.warning(f"[启动] 完成，但存在降级项: {BOOT_STATE['errors']}")
    else:
        logger.info("[启动] 初始化完成")

def execute_merge_logic(task_id, target_tmp_dir, final_out_file, log_title):
    try:
        if not os.path.exists(target_tmp_dir): raise Exception("未找到缓存目录")

        target_sub_dir, m3u8_file_path = None, None
        for root, dirs, files in os.walk(target_tmp_dir):
            if any(f.endswith(('.ts', '.m4s')) for f in files):
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
            ts_files = [f for f in os.listdir(target_sub_dir) if f.endswith('.ts') or f.endswith('.m4s') or f.endswith('.jpeg') or '.part-Frag' in f]
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
                    if task_id in tasks: tasks[task_id]['log'] = line.strip()[-60:]
        
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

def run_download(task_id, cmd):
    task_name = tasks.get(task_id, {}).get('name', 'Unknown')
    log_info(f"[调度器] 任务 [{task_name}] 开始执行")
    try:
        with TASK_LOCK: 
            tasks[task_id]['status'] = '下载中'
            tasks[task_id]['process'] = None
        save_tasks()
        
        # 设置环境变量，确保合并时使用映射目录的临时空间
        env = os.environ.copy()
        temp_dir = tasks.get(task_id, {}).get('temp_dir', CONFIG["DOWNLOAD_DIR"])
        env['TMPDIR'] = temp_dir
        
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, encoding='utf-8', errors='ignore', env=env, cwd=temp_dir)
        
        with TASK_LOCK:
            tasks[task_id]['process'] = process
        
        for line in iter(process.stdout.readline, ''):
            with TASK_LOCK:
                if task_id not in tasks:
                    break
                current_status = tasks[task_id].get('status')
                if current_status != '下载中':
                    log_info(f"[调度器] 任务 [{task_name}] 状态已变更为 {current_status}，停止读取输出")
                    break
                log_content = line.strip()
                if "%" in log_content or "B/s" in log_content or "downloading" in log_content.lower(): 
                    tasks[task_id]['log'] = log_content[-60:]
        
        process.wait()
        
        with TASK_LOCK:
            if task_id in tasks and tasks[task_id]['status'] == '下载中':
                download_dir = tasks[task_id].get('download_dir', CONFIG["DOWNLOAD_DIR"])
                temp_dir = tasks[task_id].get('temp_dir', os.path.join(download_dir, f"{task_name}_temp"))
                mp4_file = os.path.join(download_dir, f"{task_name}.mp4")
                
                if process.returncode == 0 and os.path.exists(mp4_file):
                    tasks[task_id]['status'] = '已完成'
                    tasks[task_id]['log'] = '✅ 完整下载并合并成功'
                    shutil.rmtree(temp_dir, ignore_errors=True)
                else:
                    reason = "进程报错中断" if process.returncode != 0 else "假成功(未生成MP4文件)"
                    tasks[task_id]['status'] = '错误'
                    tasks[task_id]['log'] = f'中断({reason})，可点[恢复]或[强合]'
    except Exception as e:
        log_error(f"[调度器] 任务 [{task_name}] 执行异常: {e}")
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

@app.route('/health')
def health():
    return jsonify({"status": "ok"}), 200


@app.route('/ready')
def ready():
    state = refresh_boot_state()
    ready_ok = state["downloads_ready"] and state["bin_ready"] and state["ffmpeg_ready"]
    status_code = 200 if ready_ok else 503
    return jsonify({
        "ready": ready_ok,
        "phase": state["phase"],
        "ffmpeg_ready": state["ffmpeg_ready"],
        "bin_ready": state["bin_ready"],
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
        active_workers = sum(1 for t in tasks.values() if t['status'] in ['下载中', '合并中'])
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

@app.route('/api/clear', methods=['POST'])
@requires_auth
def clear_tasks():
    with TASK_LOCK:
        to_delete = [tid for tid, t in tasks.items() if t['status'] not in ['下载中', '排队中', '合并中']]
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
        to_delete = [tid for tid in ids if tid in tasks and tasks[tid]['status'] not in ['下载中', '排队中', '合并中']]
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
        
        elif action == 'resume' and task.get('cmd'):
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
    
    save_tasks()
    return '', 200

@app.route('/down', methods=['POST'])
@requires_auth
def down():
    global GLOBAL_REFERER
    try:
        url_text = request.form.get('url', '').strip()
        raw_name = request.form.get('name', 'video').strip()
        referer_val = request.form.get('referer', '').strip()
        sub_path = request.form.get('sub_path', '').strip()
        
        if not url_text:
            return jsonify({"error": "URL不能为空"}), 400

        urls = extract_m3u8_urls(url_text)
        if not urls:
            return jsonify({"error": "没有识别到有效的 m3u8 链接"}), 400
        
        if referer_val == "https://" or not referer_val: 
            referer_val = ""
        
        # 处理下载子目录
        download_dir = CONFIG["DOWNLOAD_DIR"]
        if sub_path:
            # 清理路径，防止目录遍历
            sub_path = os.path.basename(sub_path.strip('/\\'))
            if sub_path:
                download_dir = os.path.join(download_dir, sub_path)
                os.makedirs(download_dir, exist_ok=True)
        
        with TASK_LOCK:
            active_urls = {t.get('url'): t for t in tasks.values() if t.get('status') in ACTIVE_TASK_STATUSES}
            GLOBAL_REFERER = referer_val

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
            start_task(url, name, task_id, download_dir)
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

def start_task(url, name, task_id, download_dir=None):
    global GLOBAL_REFERER
    if download_dir is None:
        download_dir = CONFIG["DOWNLOAD_DIR"]
    temp_dir = os.path.join(download_dir, f"{name}_temp")
    os.makedirs(temp_dir, exist_ok=True)
    output_file = os.path.join(download_dir, f"{name}.mp4")
    cmd = [
        CONFIG["BIN_PATH"], url,
        "-P", f"temp:{temp_dir}",
        "-o", output_file,
        "--concurrent-fragments", "10",
        "--hls-prefer-native",
        "--no-part",
        "--merge-output-format", "mp4",
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    with TASK_LOCK:
        if GLOBAL_REFERER: 
            cmd.extend(["--add-header", f"Referer:{GLOBAL_REFERER}"])
        tasks[task_id] = {
            'url': url, 
            'name': name, 
            'cmd': cmd, 
            'status': '排队中', 
            'log': '准备中...', 
            'created_at': datetime.datetime.now().isoformat(timespec='seconds'),
            'process': None,
            'download_dir': download_dir,
            'temp_dir': temp_dir
        }
    save_tasks()

def scheduler_loop():
    while True:
        try:
            with TASK_LOCK:
                ffmpeg_ready = shutil.which("ffmpeg") is not None
                
                if ffmpeg_ready:
                    for tid, t in tasks.items():
                        if t['status'] == '等待FFmpeg':
                            t['status'] = '排队中'
                            t['log'] = 'FFmpeg就绪，开始排队...'
                
                active_tasks = sum(1 for t in tasks.values() if t['status'] in ['下载中', '合并中'])
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
                        else:
                            if ffmpeg_ready:
                                threading.Thread(target=run_local_merge_tool, args=(tid, task['folder_target'])).start()
                            else:
                                task['status'] = '等待FFmpeg'
                                task['log'] = '等待FFmpeg安装完成...'
        except Exception as e:
            log_error(f"[调度器异常] {e}")
        time.sleep(1)

if __name__ == "__main__":
    logger.info("=== DDM3U8 服务启动 ===")
    boot()
    
    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()
    logger.info(f"调度器已启动，最大并发下载数: {CONFIG['MAX_DOWNLOADS']}")
    
    logger.info(f"Flask 服务启动，监听端口: {CONFIG['PORT']}")
    if WEB_USER and WEB_PASS:
        logger.info("Basic Auth 已启用")
    else:
        logger.info("Basic Auth 未启用")
    
    app.run(host='0.0.0.0', port=CONFIG["PORT"], threaded=True)