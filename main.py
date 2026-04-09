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
MAX_RETRIES = 1
COOKIE_FILE = 'tiktok_cookies.txt'
PROXIES_FILE = 'proxies.txt'

shutdown_event = threading.Event()
console = Console()
status_lock = threading.Lock()
thread_local = threading.local()

# Parse proxies globally
proxy_list = []
if os.path.exists(PROXIES_FILE):
    with open(PROXIES_FILE, 'r') as f:
        proxy_list = [line.strip() for line in f if line.strip()]
proxy_index = 0

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
    """Stateful tracking for exactly what is inside the pipeline."""
    priority: int
    url: str = field(compare=False)
    video_id: str = field(compare=False)
    status: str = field(default="Pending", compare=False)
    progress: str = field(default="-", compare=False)
    speed: str = field(default="-", compare=False)
    eta: str = field(default="-", compare=False)
    retries_left: int = field(default=MAX_RETRIES, compare=False)

class ExtractorLogger:
    def __init__(self, engine):
        self.engine = engine

    def debug(self, msg):
        if "page" in msg.lower() or "[tiktok:user]" in msg:
            clean = msg.split(":")[-1].strip()
            self.engine.update_scanning_msg(clean)

    def warning(self, msg): pass
    def error(self, msg): log_error("Scout Extractor Error", msg)

class NullLogger:
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): log_error("Pipeline Core Error", msg)

class TikTokEngine:
    """Total Overhaul (v7.0): Object-Oriented Controller Stage."""
    
    def __init__(self, username):
        self.username = username.strip().replace("@", "")
        self.user_dir = os.path.join("downloads", self.username)
        if not os.path.exists(self.user_dir):
            os.makedirs(self.user_dir)
            
        self.task_queue = queue.PriorityQueue()
        self.extractor_done = threading.Event()
        
        # Stats & Execution Stage tracking
        self.stats = {"total": 0, "completed": 0, "failed": 0, "start_time": time.time()}
        self.active_tasks = {} # Maps Future -> VideoTask
        
        # Rich UI elements
        self.progress_bar = Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
            BarColumn(), TaskProgressColumn(), TimeElapsedColumn(),
        )
        self.main_task_id = self.progress_bar.add_task("[bold yellow]Scanning Profile...[/]", total=1)
        
        self.full_clean()

    def update_scanning_msg(self, msg):
        self.progress_bar.update(self.main_task_id, description=f"[bold yellow]Scanning Profile...[/] [dim]({msg})[/]")

    def full_clean(self):
        garbage = ['*.mp3', '*.m4a', '*.webm', '*.tmp', '*.part', '*.ytdl', '*.f*']
        for ext in garbage:
            for f in glob.glob(os.path.join(self.user_dir, ext)):
                try: os.remove(f)
                except: pass

    def cleanup_err_files(self, video_id):
        try:
            for f in glob.glob(os.path.join(self.user_dir, f"*{video_id}*.*")):
                if f.endswith(('.temp', '.part', '.ytdl')):
                    os.remove(f)
        except: pass

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
        """Constructs the heavily-optimized payloads"""
        opts = {
            'format': 'best[vcodec!=none]',
            'impersonate': ImpersonateTarget.from_str('chrome'),
            'logger': ExtractorLogger(self) if is_extractor else NullLogger(),
            'quiet': True,
            'no_warnings': True,
            'ffmpeg_location': r'C:\yt-dlp',
            'socket_timeout': 15,
            'retries': 5,
            'nopart': False,
            'overwrites': True,
        }
        
        if is_extractor:
            opts['extract_flat'] = True
            opts['extractor_args'] = {'tiktok': {'web_id': 'random', 'app_info': '1180', 'api_hostname': 'api16-normal-c-useast1a.tiktokv.com'}}
            if os.path.exists(COOKIE_FILE):
                opts['cookiefile'] = COOKIE_FILE
        else:
            opts['connector_args'] = {'force_no_http2': True}
            opts['http_chunk_size'] = 2621440 # 2.5MB micro-chunks (CDN bypass payload)
            opts['concurrent_fragment_downloads'] = 4
            opts['throttledratelimit'] = 25000
            opts['progress_hooks'] = [self.master_progress_hook]
            opts['outtmpl'] = f'{self.user_dir}/%(title).50s [%(id)s].%(ext)s'
            opts['download_archive'] = os.path.join(self.user_dir, 'archive.txt')
            
        global proxy_list, proxy_index
        if proxy_list:
            with status_lock:
                opts['proxy'] = proxy_list[proxy_index % len(proxy_list)]
                proxy_index += 1
                
        return opts

    def scout_routine(self):
        """Metadata Stage: Constantly parses profile and feeds the PriorityQueue lazily."""
        try:
            with yt_dlp.YoutubeDL(self.get_ydl_opts(is_extractor=True)) as ydl:
                info_dict = ydl.extract_info(f"https://www.tiktok.com/@{self.username}", download=False)
                for e in info_dict.get('entries', []):
                    if shutdown_event.is_set(): break
                    if e and 'id' in e:
                        vid_id = e['id']
                        url = f"https://www.tiktok.com/@{self.username}/video/{vid_id}"
                        
                        # Priority 1 is standard queue
                        task = VideoTask(priority=1, url=url, video_id=vid_id)
                        self.task_queue.put(task)
                        
                        with status_lock:
                            self.stats['total'] += 1
                        self.progress_bar.update(self.main_task_id, total=self.stats['total'])
                        
                        # Anti-Ban Batch Pacing
                        if self.stats['total'] > 0 and self.stats['total'] % 100 == 0:
                            self.update_scanning_msg("Pacing Batch (Anti-Ban Sleep)...")
                            time.sleep(2)
                            
                if self.stats['total'] == 0:
                    self.progress_bar.update(self.main_task_id, total=0)
                else:
                    self.progress_bar.update(self.main_task_id, description=f"[bold green]Executing Pipeline for @{self.username}[/]")
        except Exception as e:
            import traceback
            log_error("Scout Loop Error", traceback.format_exc())
        finally:
            self.extractor_done.set()

    def execution_stage(self, task: VideoTask):
        """Execution Stage: Handles the actual file I/O safely wrapped in try/catches."""
        if shutdown_event.is_set(): return (False, task)
        
        thread_local.current_task = task
        with status_lock:
            task.status = "Initializing"
            
        try:
            # We strictly cache the YoutubeDL object per-thread inside the local context
            if getattr(thread_local, 'ydl', None) is None:
                thread_local.ydl = yt_dlp.YoutubeDL(self.get_ydl_opts(is_extractor=False))
            
            thread_local.ydl.download([task.url])
            return (True, task)
        except Exception as e:
            import traceback
            log_error(f"Execution Failed for {task.video_id}", traceback.format_exc())
            self.cleanup_err_files(task.video_id)
            return (False, task)

    def generate_ui(self):
        # Sliding Window Live Pipeline UI
        table = Table(title="[bold magenta]Live Pipeline Window[/]", expand=True, border_style="cyan")
        table.add_column("Video ID", style="dim cyan", width=20)
        table.add_column("Status", style="bold yellow")
        table.add_column("Progress", style="bold green", justify="right")
        table.add_column("Speed", style="bold blue", justify="right")
        table.add_column("ETA", style="bold red", justify="right")

        with status_lock:
            active_tasks = list(self.active_tasks.values())
            # Map out everything currently passing through the threads
            for task in active_tasks[:CONCURRENT_VIDEOS]:
                table.add_row(task.video_id, task.status, task.progress, task.speed, task.eta)

        elapsed = time.time() - self.stats["start_time"]
        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
        
        summary = (
            f"[bold cyan]Total Enqueued:[/] {self.stats['total']}  |  [bold green]Success:[/] {self.stats['completed']}  |  "
            f"[bold red]Failed:[/] {self.stats['failed']}  |  [bold yellow]Uptime:[/] {elapsed_str}"
        )
        summary_panel = Panel(Align.center(summary), style="bold white on black", border_style="green")
        
        return Group(
            Panel(self.progress_bar, title="[bold blue]Pipeline Flow Controller[/]", border_style="blue"), 
            table, 
            summary_panel
        )

    def run_pipeline(self):
        """Controller Stage: Manages the Scout, executes tasks with FIRST_COMPLETED, and maintains UI."""
        t_scout = threading.Thread(target=self.scout_routine, daemon=True)
        t_scout.start()

        with Live(self.generate_ui(), refresh_per_second=12) as live:
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
                
                while not shutdown_event.is_set():
                    # Fill the executor sliding window until full capability
                    while len(self.active_tasks) < CONCURRENT_VIDEOS:
                        try:
                            # Non-blocking pop to prevent freezing
                            task = self.task_queue.get(timeout=0.1) 
                            future = executor.submit(self.execution_stage, task)
                            with status_lock:
                                self.active_tasks[future] = task
                        except queue.Empty:
                            break

                    futures = list(self.active_tasks.keys())
                    if not futures:
                        # Extractor exhausted and no threads active = Perfect End Condition
                        if self.extractor_done.is_set():
                            time.sleep(0.5) # Final UI flush grace period
                            if self.stats['total'] > 0:
                                self.progress_bar.update(self.main_task_id, completed=self.stats['total'])
                            live.update(self.generate_ui())
                            break
                        else:
                            live.update(self.generate_ui())
                            continue

                    # The FIRST_COMPLETED zero-idle strategy
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
                                    # Priority 0: Jump instantly to the absolute front of the queue
                                    task.retries_left -= 1
                                    task.priority = 0
                                    task.status = "Requeued"
                                    task.speed = "-"
                                    task.progress = "0%"
                                    task.eta = "-"
                                    
                                    # Shove it right back into the pipeline
                                    self.task_queue.put(task)
                                else:
                                    self.stats["failed"] += 1
                                    self.progress_bar.update(self.main_task_id, advance=1)
                                    
                    live.update(self.generate_ui())
                
                # Cleanup if user aborted aggressively
                if shutdown_event.is_set():
                    for f in self.active_tasks.keys(): f.cancel()
        
        self.full_clean()
        
        elapsed = time.time() - self.stats["start_time"]
        final_report = Group(
            Panel(f"Pipeline Pipeline Completed in [bold cyan]{time.strftime('%H:%M:%S', time.gmtime(elapsed))}[/]", style="green"),
            Text(f"Target: {self.stats['total']} | Executed: {self.stats['completed']} | Fully Failed (After Retries): {self.stats['failed']}", style="bold white", justify="center")
        )
        console.print("\n")
        console.print(final_report)

def signal_handler(sig, frame):
    if shutdown_event.is_set(): os._exit(1)
    shutdown_event.set()
    console.print("\n[bold red blink]Pipeline Purge Triggered! Descaling gracefully... (Press Ctrl+C again to instantly kill)[/]")

signal.signal(signal.SIGINT, signal_handler)

def perform_update():
    current_v = yt_dlp.version.__version__
    console.print(f"[*] Current Engine Version: [bold cyan]{current_v}[/]")
    
    with console.status("[bold yellow]Contacting PyPI and fetching bleeding-edge yt-dlp core...[/]"):
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "-U", "yt-dlp[default]"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            console.print("\n[bold green]✔ Engine completely upgraded! Please restart the script to apply the core upgrades.[/]")
            os._exit(0)
        except Exception as e:
            console.print(f"\n[bold red]✖ Update failed:[/] {e}")

if __name__ == "__main__":
    console.print(Panel("[bold white]TikTok FastBulk Pipeline (v7.0) Enterprise Object-Oriented Edition[/]", style="blue"))
    u_input = console.input("[bold]Enter Username (or type 'update' to upgrade engine):[/] ")
    if u_input.strip().lower() == 'update':
        perform_update()
    elif u_input:
        engine = TikTokEngine(u_input)
        engine.run_pipeline()