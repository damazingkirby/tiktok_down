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
import shutil
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
MAX_RETRIES = 2
COOKIE_CANDIDATES = ['tiktok_cookies.txt', 'tiktok_cookie.txt']
PROXIES_FILE = 'proxies.txt'

# System-level ffmpeg detection
SYSTEM_FFMPEG = shutil.which('ffmpeg')

# Regex patterns (inspired by TikTokDownloader-master)
SEC_UID_PATTERNS = [
    re.compile(r'"verified":(?:false|true),"secUid":"([a-zA-Z0-9_-]+)"'),
    re.compile(r'"secUid":"([a-zA-Z0-9_-]{20,})"'),
    re.compile(r'sec_user_id=([a-zA-Z0-9_-]+)'),
]

# Reserved non-username folders to skip during bulk updates
RESERVED_FOLDERS = {
    'power', 'update', 'source', 'cache', '__pycache__',
    'test_clean', 'test_embed', 'test_slides', 'test_lazy'
}

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


def parse_usernames(raw_input: str):
    """Accept one or many usernames separated by spaces or commas."""
    value = (raw_input or '').strip()
    if not value:
        return []

    usernames = []
    seen = set()
    for token in re.split(r'[\s,]+', value):
        clean = token.strip().replace('@', '').strip()
        if not clean:
            continue
        if clean.lower() in {'update', 'power'}:
            continue
        if re.match(r'^[a-zA-Z0-9_.]+$', clean) and clean not in seen:
            seen.add(clean)
            usernames.append(clean)
    return usernames


def clean_msg(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', str(text)).strip()

def log_error(vid_or_msg, error_details=""):
    details = clean_msg(error_details)
    if not details:
        return
    with status_lock:
        with open('errors.log', 'a', encoding='utf-8') as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {vid_or_msg} - {details}\n")

def get_available_cookie_file():
    for f in COOKIE_CANDIDATES:
        if os.path.exists(f) and os.path.getsize(f) > 50:
            return f
    return None

# --- PIPELINE OBJECTS ---

@dataclass(order=True)
class VideoTask:
    """Stateful tracking for video and photo/slideshow items inside the pipeline."""
    priority: int
    url: str = field(compare=False)
    video_id: str = field(compare=False)
    status: str = field(default="Pending", compare=False)
    progress: str = field(default="-", compare=False)
    speed: str = field(default="-", compare=False)
    eta: str = field(default="-", compare=False)
    retries_left: int = field(default=MAX_RETRIES, compare=False)
    is_slideshow: bool = field(default=False, compare=False)

class ExtractorLogger:
    def __init__(self, engine):
        self.engine = engine

    def debug(self, msg):
        low = msg.lower()
        if "page" in low or "[tiktok:user]" in msg or "downloading webpage" in low:
            clean = msg.split(":")[-1].strip()
            self.engine.update_scanning_msg(clean)

    def warning(self, msg):
        pass

    def error(self, msg):
        log_error("Scout Extractor Error", msg)

class NullLogger:
    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        log_error("Pipeline Core Error", msg)

class TikTokEngine:
    """Refined Engine (v8.0): Universal Format Handler & Adaptive Profile Scouter."""

    def __init__(self, username, deep_scan=False):
        self.username = username.strip().replace("@", "")
        self.deep_scan = deep_scan
        self.user_dir = os.path.join("downloads", self.username)
        os.makedirs(self.user_dir, exist_ok=True)

        self.sec_uid_file = os.path.join(self.user_dir, ".sec_uid")
        self.sec_uid = self._load_cached_sec_uid()
        self.archive_file = os.path.join(self.user_dir, "archive.txt")
        self.archived_ids = self._load_archive()

        self.task_queue = queue.PriorityQueue()
        self.enqueued_ids = set()
        self.extractor_done = threading.Event()

        # Stats & Execution Stage tracking
        self.stats = {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "slideshows": 0,
            "start_time": time.time()
        }
        self.active_tasks = {}

        # Rich UI elements
        self.progress_bar = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
        )
        scan_label = "[bold red]Power-Scanning[/]" if self.deep_scan else "[bold yellow]Scanning Profile[/]"
        self.main_task_id = self.progress_bar.add_task(f"{scan_label} @{self.username}...", total=None)

        self.full_clean()

    def _load_cached_sec_uid(self):
        """Loads cached secUid from disk if available."""
        if os.path.exists(self.sec_uid_file):
            try:
                with open(self.sec_uid_file, 'r', encoding='utf-8') as f:
                    uid = f.read().strip()
                    if uid.startswith("MS4wLjABAAAA"):
                        return uid
            except Exception:
                pass
        return None

    def _save_sec_uid(self, sec_uid):
        """Caches secUid to disk for instant future scans."""
        if sec_uid and sec_uid.startswith("MS4wLjABAAAA"):
            self.sec_uid = sec_uid
            try:
                with open(self.sec_uid_file, 'w', encoding='utf-8') as f:
                    f.write(sec_uid)
            except Exception:
                pass

    def _load_archive(self):
        """Loads previously downloaded video IDs into memory for instant pre-filtering."""
        archived = set()
        if os.path.exists(self.archive_file):
            try:
                with open(self.archive_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = line.strip().split()
                        if parts:
                            archived.add(parts[-1])
            except Exception:
                pass
        return archived

    def update_scanning_msg(self, msg):
        prefix = "[bold red]Power-Scanning[/]" if self.deep_scan else "[bold yellow]Scanning Profile[/]"
        self.progress_bar.update(self.main_task_id, description=f"{prefix} [dim]({msg})[/]")

    def full_clean(self):
        """Additive cleanup: removes stray temporary fragments older than 60s."""
        garbage = ['*.tmp', '*.part', '*.ytdl']
        for ext in garbage:
            for f in glob.glob(os.path.join(self.user_dir, ext)):
                try:
                    if time.time() - os.path.getmtime(f) > 60:
                        os.remove(f)
                except Exception:
                    pass

    def cleanup_err_files(self, video_id):
        """Cleans up specifically for a failed video ID."""
        try:
            for f in glob.glob(os.path.join(self.user_dir, f"*{video_id}*.*")):
                if f.endswith(('.temp', '.part', '.ytdl')):
                    os.remove(f)
        except Exception:
            pass

    def _enqueue_video(self, entry):
        """Helper to standardize, filter, and queue discovered video or photo entries."""
        if not entry:
            return False
        vid_id = entry.get('id')
        if not vid_id:
            return False

        if vid_id in self.enqueued_ids:
            return False
        self.enqueued_ids.add(vid_id)

        # Instant in-memory pre-filtering: skip already downloaded videos
        if vid_id in self.archived_ids:
            with status_lock:
                self.stats['skipped'] += 1
            return False

        # Cache sec_uid if entry contains channel_id
        entry_channel_id = entry.get('channel_id')
        if entry_channel_id and not self.sec_uid:
            self._save_sec_uid(entry_channel_id)

        # Detect slideshow posts
        is_slideshow = (
            entry.get('vcodec') == 'none'
            or entry.get('duration') == 0
            or 'photomode' in str(entry.get('thumbnail', ''))
            or 'photo' in str(entry.get('webpage_url', ''))
        )

        url = f"https://www.tiktok.com/@{self.username}/video/{vid_id}"
        task = VideoTask(
            priority=1,
            url=url,
            video_id=vid_id,
            is_slideshow=is_slideshow
        )
        self.task_queue.put(task)

        with status_lock:
            self.stats['total'] += 1
            if is_slideshow:
                self.stats['slideshows'] += 1

        self.progress_bar.update(self.main_task_id, total=self.stats['total'])

        if self.stats['total'] % 50 == 0:
            time.sleep(0.05)
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

    def get_ydl_opts(self, is_extractor=False, use_cookies=True):
        """Constructs robust yt-dlp options with universal format support."""
        opts = {
            # Universal format selector. Prioritizes best merged video, falls back to best audio/photo post.
            'format': 'bestvideo+bestaudio/best[vcodec!=none]/best',
            'impersonate': ImpersonateTarget.from_str('chrome'),
            'logger': ExtractorLogger(self) if is_extractor else NullLogger(),
            'quiet': True,
            'no_warnings': True,
            'socket_timeout': 20 if self.deep_scan else 15,
            'retries': 6 if self.deep_scan else 4,
            'nopart': False,
            'overwrites': True,
            'writethumbnail': False,  # Disabled for maximum download speed and zero companion image clutter
            'buffersize': 1048576,    # 1MB buffer to reduce disk syscalls
            'http_chunk_size': 5242880,  # 5MB chunking for high-speed fragment streaming
            'concurrent_fragment_downloads': 5,
        }

        if SYSTEM_FFMPEG:
            opts['ffmpeg_location'] = SYSTEM_FFMPEG

        cookie_file = get_available_cookie_file()
        if use_cookies and cookie_file:
            opts['cookiefile'] = cookie_file

        if is_extractor:
            if self.deep_scan:
                opts['extract_flat'] = 'in_playlist'
                opts['lazy_playlist'] = True
                opts['playlist_items'] = '1-99999'
            else:
                opts['extract_flat'] = True
        else:
            opts['progress_hooks'] = [self.master_progress_hook]
            opts['outtmpl'] = f'{self.user_dir}/[%(id)s] %(title).50s.%(ext)s'
            opts['download_archive'] = self.archive_file

        global proxy_list, proxy_index
        if proxy_list:
            with status_lock:
                opts['proxy'] = proxy_list[proxy_index % len(proxy_list)]
                proxy_index += 1

        return opts

    def scout_routine(self):
        """Multi-stage adaptive profile scouter with secUid fast-path & fallback layers."""
        try:
            found_any = False

            def consume(info):
                nonlocal found_any
                if not info:
                    return
                # If the overall result contains channel_id, cache it
                channel_id = info.get('channel_id')
                if channel_id:
                    self._save_sec_uid(channel_id)

                entries = info.get('entries')
                if entries:
                    for e in entries:
                        if shutdown_event.is_set():
                            break
                        if self._enqueue_video(e):
                            found_any = True

            # --- STAGE 1: Direct Internal API Scan using secUid (Fastest & Most Reliable) ---
            if self.sec_uid:
                self.update_scanning_msg(f"Scanning via Cached secUid ({self.sec_uid[:15]}...)...")
                try:
                    with yt_dlp.YoutubeDL(self.get_ydl_opts(is_extractor=True)) as ydl:
                        info = ydl.extract_info(f"tiktokuser:{self.sec_uid}", download=False, process=self.deep_scan)
                        consume(info)
                except Exception as e:
                    log_error(f"Scout API Cached secUid Error (@{self.username})", str(e))

            # --- STAGE 2: Web Desktop Profile Scan (Native SlardarWAF Solver) ---
            if not found_any:
                self.update_scanning_msg("Scanning Web Profile Page...")
                try:
                    with yt_dlp.YoutubeDL(self.get_ydl_opts(is_extractor=True)) as ydl:
                        info = ydl.extract_info(f"https://www.tiktok.com/@{self.username}", download=False, process=self.deep_scan)
                        consume(info)
                except Exception as e:
                    log_error(f"Scout Web Error (@{self.username})", str(e))

            # --- STAGE 3: Anonymous Fallback without Cookies (if cookies caused WAF friction) ---
            if not found_any:
                self.update_scanning_msg("Retrying Web Profile (Anonymous Clean)...")
                try:
                    opts = self.get_ydl_opts(is_extractor=True, use_cookies=False)
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(f"https://www.tiktok.com/@{self.username}", download=False, process=False)
                        consume(info)
                except Exception as e:
                    log_error(f"Scout Clean Web Error (@{self.username})", str(e))

            # --- STAGE 4: Mobile Protocol Mimicry ---
            if not found_any:
                self.update_scanning_msg("Attempting Mobile Protocol...")
                try:
                    opts = self.get_ydl_opts(is_extractor=True, use_cookies=False)
                    opts['impersonate'] = ImpersonateTarget.from_str('chrome:android')
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(f"https://m.tiktok.com/h5/share/usr/@{self.username}", download=False, process=True)
                        consume(info)
                except Exception as e:
                    log_error(f"Scout Mobile Error (@{self.username})", str(e))

            # Status Update
            if not found_any:
                self.progress_bar.update(self.main_task_id, total=0, description=f"[bold red]@{self.username} - No videos found or profile restricted[/]")
            else:
                mode_desc = "Deep-Scan Complete" if self.deep_scan else "Pipeline Ready"
                self.progress_bar.update(self.main_task_id, description=f"[bold green]{mode_desc} @{self.username}[/]")

        except Exception as e:
            import traceback
            log_error("Critical Scout Routine Crash", traceback.format_exc())
        finally:
            self.extractor_done.set()

    def execution_stage(self, task: VideoTask):
        """Execution Stage: Downloads videos or slideshows with automatic retry and stream refresh."""
        if shutdown_event.is_set():
            return (False, task)

        thread_local.current_task = task
        with status_lock:
            task.status = "Initializing"

        # Local YDL cache per thread
        if getattr(thread_local, 'ydl', None) is None:
            thread_local.ydl = yt_dlp.YoutubeDL(self.get_ydl_opts(is_extractor=False))

        # Execution Attempt with stream refresh on 403 or format fallback
        try:
            thread_local.ydl.download([task.url])
            with status_lock:
                self.archived_ids.add(task.video_id)
            return (True, task)
        except Exception as primary_exc:
            err_str = str(primary_exc)

            # Retry attempt 1: Re-instantiate YDL if stream token expired or 403 occurred
            if "403" in err_str or "format" in err_str.lower():
                try:
                    with status_lock:
                        task.status = "Refreshing URL"
                    thread_local.ydl = yt_dlp.YoutubeDL(self.get_ydl_opts(is_extractor=False))
                    thread_local.ydl.download([task.url])
                    with status_lock:
                        self.archived_ids.add(task.video_id)
                    return (True, task)
                except Exception as fallback_exc:
                    err_str = str(fallback_exc)

            import traceback
            log_error(f"Execution Failed for {task.video_id}", traceback.format_exc())
            self.cleanup_err_files(task.video_id)
            return (False, task)

    def generate_ui(self):
        """Builds the sliding window live terminal UI."""
        table = Table(title="[bold magenta]Live Pipeline Window (v8.0 Refined)[/]", expand=True, border_style="cyan")
        table.add_column("Video ID", style="dim cyan", width=22)
        table.add_column("Type", style="bold white", width=12)
        table.add_column("Status", style="bold yellow")
        table.add_column("Progress", style="bold green", justify="right")
        table.add_column("Speed", style="bold blue", justify="right")
        table.add_column("ETA", style="bold red", justify="right")

        with status_lock:
            active_tasks = list(self.active_tasks.values())
            for task in active_tasks[:CONCURRENT_VIDEOS]:
                media_type = "[magenta]Slideshow[/]" if task.is_slideshow else "[cyan]Video[/]"
                table.add_row(task.video_id, media_type, task.status, task.progress, task.speed, task.eta)

        elapsed = time.time() - self.stats["start_time"]
        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))

        summary = (
            f"[bold cyan]Enqueued:[/] {self.stats['total']}  |  "
            f"[bold blue]Saved:[/] {self.stats['skipped']}  |  "
            f"[bold green]Success:[/] {self.stats['completed']}  |  "
            f"[bold red]Failed:[/] {self.stats['failed']}  |  "
            f"[bold magenta]Slideshows:[/] {self.stats['slideshows']}  |  "
            f"[bold yellow]Uptime:[/] {elapsed_str}"
        )
        summary_panel = Panel(Align.center(summary), style="bold white on black", border_style="green")

        return Group(
            Panel(self.progress_bar, title="[bold blue]TikTok Pipeline Controller[/]", border_style="blue"),
            table,
            summary_panel
        )

    def run_pipeline(self):
        """Controller Stage: Coordinates scout and execution workers with FIRST_COMPLETED zero-idle strategy."""
        t_scout = threading.Thread(target=self.scout_routine, daemon=True)
        t_scout.start()

        with Live(self.generate_ui(), refresh_per_second=12) as live:
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
                while not shutdown_event.is_set():
                    # Feed available workers
                    while len(self.active_tasks) < CONCURRENT_VIDEOS:
                        try:
                            task = self.task_queue.get(timeout=0.08)
                            future = executor.submit(self.execution_stage, task)
                            with status_lock:
                                self.active_tasks[future] = task
                        except queue.Empty:
                            break

                    futures = list(self.active_tasks.keys())
                    if not futures:
                        if self.extractor_done.is_set():
                            time.sleep(0.4)
                            if self.stats['total'] > 0:
                                self.progress_bar.update(self.main_task_id, completed=self.stats['total'])
                            live.update(self.generate_ui())
                            break
                        else:
                            live.update(self.generate_ui())
                            continue

                    done, _ = concurrent.futures.wait(
                        futures,
                        timeout=0.1,
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )

                    for future in done:
                        try:
                            success, task = future.result()
                        except Exception:
                            success = False
                            with status_lock:
                                task = self.active_tasks.get(future)

                        with status_lock:
                            if future in self.active_tasks:
                                del self.active_tasks[future]

                            if success:
                                self.stats["completed"] += 1
                                self.progress_bar.update(self.main_task_id, advance=1)
                            else:
                                if task and task.retries_left > 0:
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
                    for f in self.active_tasks.keys():
                        f.cancel()

        self.full_clean()

        elapsed = time.time() - self.stats["start_time"]
        final_report = Group(
            Panel(f"Pipeline Completed in [bold cyan]{time.strftime('%H:%M:%S', time.gmtime(elapsed))}[/]", style="green"),
            Text(
                f"Target: {self.stats['total']} | Already Saved: {self.stats['skipped']} | Executed: {self.stats['completed']} | Slideshows: {self.stats['slideshows']} | Failed: {self.stats['failed']}",
                style="bold white",
                justify="center"
            )
        )
        console.print("\n")
        console.print(final_report)

def signal_handler(sig, frame):
    if shutdown_event.is_set():
        os._exit(1)
    shutdown_event.set()
    console.print("\n[bold red blink]Pipeline Purge Triggered! Descaling gracefully... (Press Ctrl+C again to force exit)[/]")

signal.signal(signal.SIGINT, signal_handler)

def perform_update():
    """Updates yt-dlp to latest git commit using uv."""
    current_v = yt_dlp.version.__version__
    console.print(f"[*] Current Engine Version: [bold cyan]{current_v}[/]")

    with console.status("[bold yellow]Contacting GitHub and fetching latest nightly commit via uv...[/]"):
        try:
            subprocess.check_call(
                ["uv", "lock", "--upgrade-package", "yt-dlp"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            subprocess.check_call(
                ["uv", "sync"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            console.print("\n[bold green]✔ Engine upgraded to the latest GitHub version! Please restart to apply.[/]")
            sys.exit(0)
        except Exception as e:
            console.print(f"\n[bold red]✖ Update failed:[/] {e}")

def perform_bulk_update():
    """Scans valid profile directories in downloads/ and refreshes them sequentially."""
    dl_dir = "downloads"
    if not os.path.exists(dl_dir):
        console.print(Panel("[bold red]ERROR: No 'downloads' directory found![/]", style="red"))
        return

    # Strict filtering to avoid bogus non-username folders
    usernames = []
    for d in sorted(os.listdir(dl_dir)):
        p = os.path.join(dl_dir, d)
        if not os.path.isdir(p):
            continue
        clean_d = d.strip()
        if not clean_d or clean_d.lower() in RESERVED_FOLDERS:
            continue
        if clean_d.startswith(('.', '_')) and not clean_d.startswith('_jeon'):
            # Keep recognized valid usernames like _jeon.ian___, skip system folders
            if clean_d in ('.vscode', '.venv', '.git', '__pycache__'):
                continue
        # TikTok usernames only contain alphanumeric characters, underscores, and periods
        if re.match(r'^[a-zA-Z0-9_.]+$', clean_d):
            usernames.append(clean_d)

    if not usernames:
        console.print(Panel("[bold yellow]No valid profile folders found in 'downloads/'.[/]", style="yellow"))
        return

    console.print(Panel(
        f"[bold cyan]Bulk Update Core Activated: Found {len(usernames)} valid profiles[/]",
        title="TikTok Bulk Updater (v8.0)",
        border_style="green"
    ))

    summary_stats = []
    total_new_vids = 0

    for i, user in enumerate(usernames, 1):
        if shutdown_event.is_set():
            break
        console.print(f"\n[bold magenta]━━━━━━━━ Processing {i}/{len(usernames)}: @{user} ━━━━━━━━[/]")
        try:
            engine = TikTokEngine(user, deep_scan=True)
            engine.run_pipeline()
            new_vids = engine.stats.get('completed', 0)
            failed_vids = engine.stats.get('failed', 0)
            summary_stats.append((user, new_vids, failed_vids))
            total_new_vids += new_vids
        except Exception as e:
            console.print(f"[bold red]✖ Failed @{user}: {e}[/]")
            summary_stats.append((user, "ERROR", 0))

        if i < len(usernames) and not shutdown_event.is_set():
            time.sleep(1.5)

    table = Table(title="[bold green]Bulk Update Final Summary[/]", expand=True, border_style="green")
    table.add_column("Profile (Username)", style="cyan")
    table.add_column("New Items Processed", justify="right", style="bold green")
    table.add_column("Failed", justify="right", style="bold red")

    for user, new, failed in summary_stats:
        table.add_row(f"@{user}", str(new), str(failed))

    console.print("\n")
    console.print(table)
    console.print(Panel(
        f"[bold green]Bulk update complete! Total Items across all profiles: {total_new_vids}[/]",
        border_style="bold green"
    ))

if __name__ == "__main__":
    console.print(Panel(
        "[bold white]TikTok FastBulk Pipeline (v8.0 Refined Edition)[/]\n"
        "[dim]Universal Video & Photo/Slideshow Support | Smart secUid Cache | Zero Format Error[/]",
        style="blue"
    ))
    u_input = console.input("[bold]Enter Username (or type 'update' or 'power'):[/] ")
    choice = u_input.strip().lower()

    if choice == 'update':
        perform_update()
    elif choice == 'power':
        perform_bulk_update()
    else:
        usernames = parse_usernames(u_input)
        if not usernames:
            console.print(Panel("[bold yellow]No valid usernames supplied.[/]", style="yellow"))
        elif len(usernames) == 1:
            engine = TikTokEngine(usernames[0], deep_scan=False)
            engine.run_pipeline()
        else:
            console.print(Panel(
                f"[bold cyan]Queued {len(usernames)} profiles from a single input.[/]",
                title="Multi-Profile Download",
                border_style="cyan"
            ))
            for index, username in enumerate(usernames, 1):
                console.print(f"\n[bold magenta]━━━━━━━━ Profile {index}/{len(usernames)}: @{username} ━━━━━━━━[/]")
                engine = TikTokEngine(username, deep_scan=False)
                engine.run_pipeline()
                if index < len(usernames):
                    time.sleep(1)

