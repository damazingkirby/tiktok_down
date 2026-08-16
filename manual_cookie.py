import yt_dlp
import os
import signal
import time
import glob
import concurrent.futures
import threading
import queue
import re
import sys
import subprocess
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from yt_dlp.networking.impersonate import ImpersonateTarget

# Rich UI Imports
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.table import Table
from rich.align import Align
from rich.text import Text

# --- GLOBALS & CONFIG ---
CONCURRENT_VIDEOS = 15
MAX_RETRIES = 3                           # bumped from 2
COOKIE_FILE = 'tiktok_cookie.txt'
PROXIES_FILE = 'proxies.txt'

BROWSER_PROFILE = 'chrome'

shutdown_event = threading.Event()
console = Console()
status_lock = threading.Lock()
thread_local = threading.local()

proxy_list = []
if os.path.exists(PROXIES_FILE):
    with open(PROXIES_FILE, 'r') as f:
        proxy_list = [line.strip() for line in f if line.strip()]
thread_local.proxy_index = 0

def clean_msg(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', str(text)).strip()

def log_error(vid_or_msg, error_details=""):
    details = clean_msg(error_details)
    if not details: return
    with status_lock:
        with open('errors.log', 'a', encoding='utf-8') as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {vid_or_msg} - {details}\n")

# --- PIPELINE OBJECTS ---

@dataclass(order=True)
class VideoTask:
    priority: int
    url: str = field(compare=False)
    video_id: str = field(compare=False)
    status: str = field(default="Pending", compare=False)
    progress: str = field(default="-", compare=False)
    speed: str = field(default="-", compare=False)
    eta: str = field(default="-", compare=False)
    retries_left: int = field(default=MAX_RETRIES, compare=False)
    title: str = field(default="", compare=False)

class ExtractorLogger:
    def __init__(self, engine):
        self.engine = engine

    def debug(self, msg):
        if "page" in msg.lower() or "[tiktok:user]" in msg or "Downloading API" in msg:
            clean = msg.split(":")[-1].strip()
            self.engine.update_scanning_msg(clean)

    def warning(self, msg):
        log_error("Scout Extractor Warning", msg)

    def error(self, msg): 
        log_error("Scout Extractor Error", msg)

class NullLogger:
    def debug(self, msg): pass
    def warning(self, msg):
        log_error("Pipeline Core Warning", msg)
    def error(self, msg): log_error("Pipeline Core Error", msg)

class TikTokEngine:
    """Full-History Deep Crawler Edition (v8.2) with retry-with-backoff for both scout and execution."""
    
    def __init__(self, username, deep_scan=True):
        self.username = username.strip().replace("@", "")
        self.deep_scan = deep_scan
        self.user_dir = os.path.join("downloads", self.username)
        if not os.path.exists(self.user_dir):
            os.makedirs(self.user_dir)
            
        self.task_queue = queue.PriorityQueue()
        self.enqueued_ids = set()
        self.extractor_done = threading.Event()
        
        self.stats = {"total": 0, "completed": 0, "failed": 0, "start_time": time.time()}
        self.active_tasks = {}
        
        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
            BarColumn(), TaskProgressColumn(), TimeElapsedColumn(),
        )
        self.main_task_id = self.progress_bar.add_task(f"[bold red]Deep-Crawling Full History[/] @{self.username}...", total=None)
        
        self.full_clean()
        self.cookie_status = self.resolve_cookies()

    def resolve_cookies(self):
        cookie_candidates = [COOKIE_FILE, 'tiktok_cookies.txt', 'cookies.txt']
        for cand in cookie_candidates:
            if os.path.exists(cand):
                console.print(f"[green]✓ Cookies file loaded ({cand})[/]")
                return ('file', cand)
        console.print(f"[cyan]ℹ No cookie file found. Attempting safe native extraction from {BROWSER_PROFILE}...[/]")
        return ('browser', BROWSER_PROFILE)

    def update_scanning_msg(self, msg):
        self.progress_bar.update(self.main_task_id, description=f"[bold red]Deep-Crawling Full History[/] [dim]({msg})[/]")

    def full_clean(self):
        garbage = ['*.tmp', '*.part', '*.ytdl']
        for ext in garbage:
            for f in glob.glob(os.path.join(self.user_dir, ext)):
                try: 
                    if time.time() - os.path.getmtime(f) > 60:
                        os.remove(f)
                except: pass

    def cleanup_err_files(self, video_id):
        try:
            for f in glob.glob(os.path.join(self.user_dir, f"*{video_id}*.*")):
                if f.endswith(('.temp', '.part', '.ytdl')):
                    os.remove(f)
        except: pass

    def _enqueue_video(self, vid_id, custom_url=None, title=""):
        if not vid_id: return False
        if vid_id in self.enqueued_ids:
            return False
        self.enqueued_ids.add(vid_id)
        
        url = custom_url or f"https://www.tiktok.com/@{self.username}/video/{vid_id}"
        task = VideoTask(priority=1, url=url, video_id=vid_id, title=title)
        self.task_queue.put(task)
        
        with status_lock:
            self.stats['total'] += 1
        self.progress_bar.update(self.main_task_id, total=self.stats['total'])
        return True

    def master_progress_hook(self, d):
        if shutdown_event.is_set():
            raise ValueError("Pipeline Abort Triggered")
            
        task = getattr(thread_local, 'current_task', None)
        if task:
            if d['status'] == 'downloading':
                with status_lock:
                    task.status = "Downloading"
                    task.speed = d.get('_speed_str', 'N/A')
                    task.progress = d.get('_percent_str', '  0%')
                    task.eta = d.get('_eta_str', 'N/A')
            elif d['status'] == 'finished':
                with status_lock:
                    task.status = "Finalizing"
                    task.speed = "-"
                    task.progress = "100%"
                    task.eta = "-"

    def get_ydl_opts(self, is_extractor=False):
        opts = {
            'format': 'best[vcodec!=none]',
            'impersonate': ImpersonateTarget.from_str('chrome-133:macos-15'),
            'logger': ExtractorLogger(self) if is_extractor else NullLogger(),
            'quiet': False,
            'no_warnings': False,
            'socket_timeout': 30,
            'retries': 10,
            'nopart': False,
            'overwrites': True,
        }
        
        ctype, cval = self.cookie_status
        if ctype == 'file':
            opts['cookiefile'] = cval
        else:
            try:
                opts['cookiesfrombrowser'] = (cval,)
            except Exception as e:
                log_error("Cookie Browser Extraction Error", str(e))

        if is_extractor:
            # Force full pagination across the entire profile history
            opts['extract_flat'] = 'in_playlist'
            opts['lazy_playlist'] = True
            opts['playlist_items'] = '1-99999'
        else:
            opts['connector_args'] = {'force_no_http2': True}
            opts['http_chunk_size'] = 2621440
            opts['concurrent_fragment_downloads'] = 4
            opts['progress_hooks'] = [self.master_progress_hook]
            opts['outtmpl'] = f'{self.user_dir}/[%(id)s] %(title).50s.%(ext)s'
            opts['download_archive'] = os.path.join(self.user_dir, 'archive.txt')
            
        if proxy_list:
            if not hasattr(thread_local, 'proxy_index'):
                thread_local.proxy_index = 0
            opts['proxy'] = proxy_list[thread_local.proxy_index % len(proxy_list)]
            thread_local.proxy_index += 1
                
        return opts

    def scout_routine(self):
        """Profile extraction with retry-with-backoff (up to 3 attempts)."""
        try:
            found_any = False
            
            def try_extract(url, max_attempts=3):
                for attempt in range(max_attempts):
                    if shutdown_event.is_set():
                        return None
                    try:
                        opts = self.get_ydl_opts(is_extractor=True)
                        with yt_dlp.YoutubeDL(opts) as ydl:
                            info = ydl.extract_info(url, download=False, process=True)
                            return info
                    except Exception as e:
                        err_str = str(e)
                        log_error(f"Extraction failed for {url} (attempt {attempt+1}/{max_attempts})", err_str)
                        if attempt < max_attempts - 1:
                            delay = min(5 * (2 ** (attempt + 1)), 30)  # 10s, 20s, 30s
                            self.update_scanning_msg(f"Challenge hit, retrying in {delay}s...")
                            time.sleep(delay)
                        else:
                            console.print(f"\n[bold red]Extractor Error after {max_attempts} attempts:[/] {err_str}")
                return None

            # --- Full Profile Deep Pagination Crawl ---
            self.update_scanning_msg("Paging through entire profile API history...")
            info = try_extract(f"https://www.tiktok.com/@{self.username}")
            if info:
                entries = info.get('entries', []) if isinstance(info, dict) else info
                for e in entries:
                    if e and 'id' in e:
                        t = e.get('title', '') if isinstance(e, dict) else ''
                        if self._enqueue_video(e['id'], title=t):
                            found_any = True

            if not found_any:
                self.progress_bar.update(
                    self.main_task_id, 
                    total=0, 
                    description=f"[bold red]@{self.username} - No Videos Discovered[/]"
                )
                console.print(f"\n[bold red]Error: Could not retrieve videos for @{self.username}. Check your cookie file validity or profile restrictions.[/]")
            else:
                self.progress_bar.update(
                    self.main_task_id, 
                    description=f"[bold green]Full History Crawl Ready @{self.username} ({self.stats['total']} total found)[/]"
                )
                    
        except Exception as e:
            import traceback
            log_error("Critical Scout Routine Crash", traceback.format_exc())
        finally:
            self.extractor_done.set()

    def execution_stage(self, task: VideoTask):
        if shutdown_event.is_set(): return (False, task)

        # --- Backoff before a retry ---
        # Only the first attempt (retries_left == MAX_RETRIES) skips the delay.
        attempt_number = MAX_RETRIES - task.retries_left
        if attempt_number > 0:
            delay = min(5 * (2 ** attempt_number), 30)  # 10s, 20s, 30s, capped
            with status_lock:
                task.status = f"Retry wait ({delay}s)"
            time.sleep(delay)
            if shutdown_event.is_set(): return (False, task)

        thread_local.current_task = task
        with status_lock:
            task.status = "Initializing"
            
        try:
            if getattr(thread_local, 'ydl', None) is None:
                thread_local.ydl = yt_dlp.YoutubeDL(self.get_ydl_opts(is_extractor=False))
            
            try:
                thread_local.ydl.download([task.url])
            except Exception as e_primary:
                if task.video_id:
                    embed_url = f"https://www.tiktok.com/embed/v2/{task.video_id}"
                    log_error(f"Primary URL failed for {task.video_id}, trying embed URL fallback: {embed_url}", str(e_primary))
                    embed_opts = self.get_ydl_opts(is_extractor=False)
                    # Force video-first: embed returns multiple streams, pick best with video codec
                    embed_opts['format'] = 'best[vcodec!=none]/best'
                    # Don't let yt-dlp auto-write html5mediaembed entries — we write manually below
                    embed_opts.pop('download_archive', None)
                    v_title = getattr(task, 'title', '')
                    safe_title = re.sub(r'[^\w\s-]', '', v_title).strip()[:50] if v_title else ''
                    if not safe_title:
                        safe_title = 'TikTok Video'
                    embed_opts['outtmpl'] = os.path.join(self.user_dir, f'[{task.video_id}] {safe_title}.%(ext)s')
                    embed_opts['playlist_items'] = '1'
                    embed_opts['noplaylist'] = True
                    
                    with yt_dlp.YoutubeDL(embed_opts) as ydl_embed:
                        ydl_embed.download([embed_url])
                    
                    # Auto-rename .unknown_video to .mp4
                    for f in glob.glob(os.path.join(self.user_dir, f"*{task.video_id}*.unknown_video")):
                        new_f = f[:-14] + ".mp4"
                        if not os.path.exists(new_f):
                            os.rename(f, new_f)
                    
                    archive_path = os.path.join(self.user_dir, 'archive.txt')
                    try:
                        with open(archive_path, 'a', encoding='utf-8') as f_arch:
                            f_arch.write(f"tiktok {task.video_id}\n")
                    except Exception:
                        pass
                else:
                    raise e_primary
            return (True, task)
        except Exception as e:
            import traceback
            log_error(f"Execution Failed for {task.video_id}", traceback.format_exc())
            self.cleanup_err_files(task.video_id)
            return (False, task)

    def generate_ui(self):
        table = Table(title="[bold magenta]Live Pipeline Window[/]", expand=True, border_style="cyan")
        table.add_column("Video ID", style="dim cyan", width=20)
        table.add_column("Status", style="bold yellow")
        table.add_column("Progress", style="bold green", justify="right")
        table.add_column("Speed", style="bold blue", justify="right")
        table.add_column("ETA", style="bold red", justify="right")

        with status_lock:
            active_tasks = list(self.active_tasks.values())
            for task in active_tasks[:CONCURRENT_VIDEOS]:
                table.add_row(task.video_id, task.status, task.progress, task.speed, task.eta)

        elapsed = time.time() - self.stats["start_time"]
        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
        
        summary = (
            f"[bold cyan]Total Enqueued:[/] {self.stats['total']}  |  [bold green]Success (New/Remaining):[/] {self.stats['completed']}  |  "
            f"[bold red]Failed:[/] {self.stats['failed']}  |  [bold yellow]Uptime:[/] {elapsed_str}"
        )
        summary_panel = Panel(Align.center(summary), style="bold white on black", border_style="green")
        
        return Group(
            Panel(self.progress_bar, title="[bold blue]Pipeline Flow Controller v8.2 (with Retry Backoff)[/]", border_style="blue"), 
            table, 
            summary_panel
        )

    def run_pipeline(self):
        t_scout = threading.Thread(target=self.scout_routine, daemon=True)
        t_scout.start()

        with Live(self.generate_ui(), refresh_per_second=12) as live:
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
                
                while not shutdown_event.is_set():
                    while len(self.active_tasks) < CONCURRENT_VIDEOS:
                        try:
                            task = self.task_queue.get(timeout=0.1) 
                            future = executor.submit(self.execution_stage, task)
                            with status_lock:
                                self.active_tasks[future] = task
                        except queue.Empty:
                            break

                    futures = list(self.active_tasks.keys())
                    if not futures:
                        if self.extractor_done.is_set():
                            time.sleep(0.5)
                            if self.stats['total'] > 0:
                                self.progress_bar.update(self.main_task_id, completed=self.stats['total'])
                            live.update(self.generate_ui())
                            break
                        else:
                            live.update(self.generate_ui())
                            continue

                    done, _ = concurrent.futures.wait(futures, timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
                    
                    for future in done:
                        try:
                            success, task = future.result()
                        except Exception:
                            success = False
                            with status_lock: task = self.active_tasks[future]
                            
                        with status_lock:
                            del self.active_tasks[future]
                            
                            if success:
                                self.stats["completed"] += 1
                                self.progress_bar.update(self.main_task_id, advance=1)
                            else:
                                if task.retries_left > 0:
                                    task.retries_left -= 1
                                    task.priority = 0
                                    task.status = "Requeued"
                                    task.speed = "-"
                                    task.progress = "0%"
                                    task.eta = "-"
                                    self.task_queue.put(task)
                                else:
                                    self.stats["failed"] += 1
                                    self.progress_bar.update(self.main_task_id, advance=1)
                                    
                    live.update(self.generate_ui())
                
                if shutdown_event.is_set():
                    for f in self.active_tasks.keys(): f.cancel()
        
        self.full_clean()
        
        elapsed = time.time() - self.stats["start_time"]
        final_report = Group(
            Panel(f"Pipeline Completed in [bold cyan]{time.strftime('%H:%M:%S', time.gmtime(elapsed))}[/]", style="green"),
            Text(f"Discovered Total: {self.stats['total']} | Processed/Skipped: {self.stats['completed']} | Failed: {self.stats['failed']}", style="bold white", justify="center")
        )
        console.print("\n")
        console.print(final_report)

def signal_handler(sig, frame):
    if shutdown_event.is_set(): os._exit(1)
    shutdown_event.set()
    console.print("\n[bold red blink]Pipeline Purge Triggered! Descaling gracefully... (Press Ctrl+C again to instantly kill)[/]")

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    console.print(Panel("[bold white]TikTok Full-History Deep Crawler (v8.2) with Retry Backoff[/]", style="blue"))
    u_input = console.input("[bold]Enter Username:[/] ")
    if u_input.strip():
        engine = TikTokEngine(u_input, deep_scan=True)
        engine.run_pipeline()