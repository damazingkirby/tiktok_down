import yt_dlp
import os
import signal
import time
import glob
import concurrent.futures
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from yt_dlp.networking.impersonate import ImpersonateTarget

# Rich UI Imports
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.table import Table
from rich.align import Align
from rich.text import Text

# --- CONFIGURATION ---
CONCURRENT_VIDEOS = 15  # Increased massively due to optimizations
COOKIE_FILE = 'tiktok_cookies.txt'
CHROME_TARGET = ImpersonateTarget.from_str('chrome')
shutdown_event = Event()
console = Console()

# UI State
stats = {
    "total": 0, "completed": 0, "failed": 0, "start_time": 0,
}

worker_status = {i: {'vid': '-', 'status': 'Idle', 'speed': '-', 'percent': '-', 'eta': '-'} for i in range(CONCURRENT_VIDEOS)}
status_lock = Lock()

slot_queue = queue.Queue()
for i in range(CONCURRENT_VIDEOS):
    slot_queue.put(i)

thread_local = threading.local()

overall_progress = Progress(
    SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
    BarColumn(), TaskProgressColumn(), TimeElapsedColumn(),
)
main_task = None

class NullLogger:
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass

def get_thread_slot_id():
    if getattr(thread_local, 'slot_id', None) is None:
        thread_local.slot_id = slot_queue.get()
    return thread_local.slot_id

def master_progress_hook(d):
    if shutdown_event.is_set():
        raise ValueError("Download Cancelled by User")
    
    if getattr(thread_local, 'slot_id', None) is not None:
        if d['status'] == 'downloading':
            with status_lock:
                worker_status[thread_local.slot_id].update({
                    'status': 'Downloading',
                    'speed': d.get('_speed_str', 'N/A'),
                    'percent': d.get('_percent_str', '  0%'),
                    'eta': d.get('_eta_str', 'N/A')
                })
        elif d['status'] == 'finished':
            with status_lock:
                worker_status[thread_local.slot_id].update({
                    'status': 'Finalizing/Verifying',
                    'speed': '-', 'percent': '100%', 'eta': '-'
                })

def get_ydl_opts(user_dir, is_extractor=False):
    opts = {
        'format': 'best[vcodec!=none]', # Enforces having a video stream! Instantly rejects audio-only photo posts
        'impersonate': CHROME_TARGET,
        'logger': NullLogger(), # Silences all terminal output, including hard errors
        'quiet': True,
        'no_warnings': True,
        'extractor_args': {'tiktok': {'web_id': 'random', 'app_info': '1180'}},
        'socket_timeout': 15,
        'retries': 3,
        'nopart': True,
        'overwrites': True,
        # Bypasses TikTok's notorious end-of-file CDN speed limit penalty (drops to 2kbps)
        'throttledratelimit': 100000, # If speed drops below 100 KB/s, reconnect instantly!
        'http_chunk_size': 1048576,   # Break files into incredibly small 1MB chunks so the CDN never flags length
        'concurrent_fragment_downloads': 5, # Download those 1MB pieces parallelly
        'progress_hooks': [master_progress_hook] if not is_extractor else [],
    }
    if os.path.exists(COOKIE_FILE):
        opts['cookiefile'] = COOKIE_FILE
        
    if is_extractor:
        opts['extract_flat'] = True
    else:
        opts.update({
            'outtmpl': f'{user_dir}/%(title).50s [%(id)s].%(ext)s',
            'download_archive': os.path.join(user_dir, 'archive.txt'),
        })
    return opts

def cleanup_err_files(user_dir, video_id):
    try:
        for f in glob.glob(os.path.join(user_dir, f"*{video_id}*.*")):
            if f.endswith('.temp') or f.endswith('.part') or f.endswith('.ytdl'):
                os.remove(f)
    except: pass

def download_worker(url, user_dir):
    if shutdown_event.is_set(): return False
    
    slot_id = get_thread_slot_id()
    video_id = url.split('/')[-1]
    
    with status_lock:
        worker_status[slot_id].update({
            'vid': video_id, 'status': 'Initializing', 'speed': '-', 'percent': '0%', 'eta': '-'
        })
        
    try:
        # Cache ydl instance per thread to eliminate initialization latency
        if getattr(thread_local, 'ydl', None) is None:
            thread_local.ydl = yt_dlp.YoutubeDL(get_ydl_opts(user_dir))
            
        thread_local.ydl.download([url])
        return True
    except Exception:
        cleanup_err_files(user_dir, video_id)
        return False
    finally:
        with status_lock:
            worker_status[slot_id] = {'vid': '-', 'status': 'Idle', 'speed': '-', 'percent': '-', 'eta': '-'}

def generate_dashboard():
    table = Table(title="[bold magenta]Active Worker Analytics[/]", expand=True, border_style="cyan")
    table.add_column("Worker Thread", style="dim", width=15)
    table.add_column("Video ID", style="dim cyan", width=20)
    table.add_column("Status", style="bold yellow")
    table.add_column("Progress", style="bold green", justify="right")
    table.add_column("Speed", style="bold blue", justify="right")
    table.add_column("ETA", style="bold red", justify="right")

    with status_lock:
        for slot_id in range(CONCURRENT_VIDEOS):
            d = worker_status[slot_id]
            table.add_row(f"Thread-{slot_id+1:02d}", d['vid'], d['status'], d['percent'], d['speed'], d['eta'])

    elapsed = time.time() - stats["start_time"] if stats["start_time"] else 0
    elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    
    summary_text = (
        f"[bold cyan]Total Videos:[/] {stats['total']}  |  [bold green]Success:[/] {stats['completed']}  |  "
        f"[bold red]Failed (Photo Posts):[/] {stats['failed']}  |  [bold yellow]Time Elapsed:[/] {elapsed_str}"
    )
    
    summary_panel = Panel(Align.center(summary_text), style="bold white on black", border_style="green")
    
    return Group(Panel(overall_progress, title="[bold blue]Overall Job Progress[/]", border_style="blue"), table, summary_panel)

def full_clean(user_dir):
    garbage = ['*.mp3', '*.m4a', '*.webm', '*.tmp', '*.part', '*.ytdl', '*.f*']
    for ext in garbage:
        for f in glob.glob(os.path.join(user_dir, ext)):
            try: os.remove(f)
            except: pass

def fast_bulk_download(input_name):
    global main_task
    username = input_name.strip().replace("@", "")
    user_dir = os.path.join("downloads", username)
    if not os.path.exists(user_dir): os.makedirs(user_dir)
    
    full_clean(user_dir)
    console.print(f"[*] Fetching video metadata for [bold yellow]@{username}[/]...")

    video_urls = []
    with yt_dlp.YoutubeDL(get_ydl_opts(user_dir, is_extractor=True)) as ydl:
        info = ydl.extract_info(f"https://www.tiktok.com/@{username}", download=False)
        video_urls = [f"https://www.tiktok.com/@{username}/video/{e['id']}" for e in info.get('entries', []) if e]

    if not video_urls:
         console.print("[red]No videos found. Check username or cookies.[/]")
         return

    stats["total"] = len(video_urls)
    stats["start_time"] = time.time()
    main_task = overall_progress.add_task(f"Downloading @{username}", total=stats["total"])

    with Live(generate_dashboard(), refresh_per_second=10) as live:
        with ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
            future_to_url = {executor.submit(download_worker, url, user_dir): url for url in video_urls}
            not_done = set(future_to_url.keys())
            
            while not_done and not shutdown_event.is_set():
                live.update(generate_dashboard())
                done, not_done = concurrent.futures.wait(not_done, timeout=0.25)
                
                for future in done:
                    try: success = future.result()
                    except Exception: success = False
                    
                    if success: stats["completed"] += 1
                    else: stats["failed"] += 1
                    overall_progress.update(main_task, advance=1)
            
            if shutdown_event.is_set():
                for future in not_done: future.cancel()

    full_clean(user_dir)
    elapsed = time.time() - stats["start_time"]
    elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    
    final_report = Group(
        Panel(f"Download Session Completed in [bold cyan]{elapsed_str}[/]", style="green"),
        Text(f"Total: {stats['total']} | Success: {stats['completed']} | Failed (Likely Photo Posts): {stats['failed']}", style="bold white", justify="center")
    )
    console.print("\n")
    console.print(final_report)

def signal_handler(sig, frame):
    if shutdown_event.is_set(): os._exit(1)
    shutdown_event.set()
    console.print("\n[bold red blink]Abort signal received! Stopping downloads gracefully... (Press Ctrl+C again to force exit)[/]")

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    console.print(Panel("[bold white]TikTok FastBulk: RICH ANALYTICS EDITION v4.0[/]", style="blue"))
    u_input = console.input("[bold]Enter Username:[/] ")
    if u_input:
        fast_bulk_download(u_input)