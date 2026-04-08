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

class ExtractorLogger:
    def debug(self, msg):
        # yt-dlp outputs something like: "[tiktok:user] soamelodie: Downloading API JSON page 4"
        if "page" in msg.lower() or "[tiktok:user]" in msg:
            # We strip the internal prefix to make it look clean
            clean_msg = msg.split(":")[-1].strip()
            overall_progress.update(main_task, description=f"[bold yellow]Scanning Profile...[/] [dim]({clean_msg})[/]")

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
        'logger': ExtractorLogger() if is_extractor else NullLogger(), # Feed API logs to UI during extraction!
        'quiet': True,
        'no_warnings': True,
        'ffmpeg_location': r'C:\yt-dlp', # Explicitly hooks into your local ffmpeg binary
        'extractor_args': {'tiktok': {
            'web_id': 'random', 
            'app_info': '1180',
            'api_hostname': 'api16-normal-c-useast1a.tiktokv.com'
        }},
        'connector_args': {'force_no_http2': True}, # CRITICAL FOR PARALLEL CHUNKS!
        'socket_timeout': 15,
        'retries': 5,
        'nopart': False,
        'overwrites': True,
        # Micro-chunks bypass the CDN throttle!
        'http_chunk_size': 2621440,   # 2.5MB micro-chunks
        'concurrent_fragment_downloads': 4, # 4 parallel sockets
        'throttledratelimit': 25000, 
        'progress_hooks': [master_progress_hook] if not is_extractor else [],
    }
    if os.path.exists(COOKIE_FILE):
        # MASSIVE SPEED FIX: Only the extractor needs cookies to get the URLs. 
        # By removing cookies from the 15 worker threads, we stop Windows from constantly 
        # locking the file and stalling all your CPU threads!
        if is_extractor:
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

    stats["total"] = 0
    stats["start_time"] = time.time()
    # Let the user know we are specifically scanning first!
    main_task = overall_progress.add_task(f"[bold yellow]Scanning Profile...[/]", total=1) 
    
    url_queue = queue.Queue()
    extractor_done = threading.Event()

    def extractor_producer():
        try:
            with yt_dlp.YoutubeDL(get_ydl_opts(user_dir, is_extractor=True)) as ydl:
                info = ydl.extract_info(f"https://www.tiktok.com/@{username}", download=False)
                # By dropping the list comprehension, this is evaluated lazily as a generator
                for e in info.get('entries', []):
                    if shutdown_event.is_set(): break
                    if e and 'id' in e:
                        url_queue.put(f"https://www.tiktok.com/@{username}/video/{e['id']}")
                        
                        # Dynamically push the target completion forward as metadata discovers new files!
                        with status_lock:
                            stats['total'] += 1
                        overall_progress.update(main_task, total=stats['total'])
                        
                        # Edge case for an empty profile instantly ending
                if stats['total'] == 0:
                    overall_progress.update(main_task, total=0)
                else:
                    # Switch the text back to downloading mode once we have all the URLs!
                    overall_progress.update(main_task, description=f"[bold green]Downloading @{username}[/]")
        except Exception:
            pass
        finally:
            extractor_done.set()

    t_producer = threading.Thread(target=extractor_producer)
    t_producer.daemon = True
    t_producer.start()

    with Live(generate_dashboard(), refresh_per_second=10) as live:
        with ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
            not_done = set()
            
            while not shutdown_event.is_set():
                # Rapidly inject new URLs into free threaded worker slots instantly
                while len(not_done) < CONCURRENT_VIDEOS:
                    try:
                        url = url_queue.get_nowait()
                        future = executor.submit(download_worker, url, user_dir)
                        not_done.add(future)
                    except queue.Empty:
                        break
                
                # Exit condition: Extractor is fully exhausted, queue is empty, and workers are done
                if extractor_done.is_set() and url_queue.empty() and len(not_done) == 0:
                    if stats['total'] > 0: overall_progress.update(main_task, completed=stats['total'])
                    live.update(generate_dashboard())
                    break
                
                live.update(generate_dashboard())
                
                if not_done:
                    # Let the workers do their magic for exactly one tick of the UI (10 FPS)
                    done, not_done = concurrent.futures.wait(not_done, timeout=0.1)
                    
                    for future in done:
                        try: success = future.result()
                        except Exception: success = False
                        
                        with status_lock:
                            if success: stats["completed"] += 1
                            else: stats["failed"] += 1
                            
                        overall_progress.update(main_task, advance=1)
                else:
                    # If workers are empty and extractor is catching up at the very beginning
                    time.sleep(0.1)
            
            if shutdown_event.is_set():
                for future in not_done: future.cancel()

    full_clean(user_dir)
    elapsed = time.time() - stats["start_time"]
    elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    
    final_report = Group(
        Panel(f"Streaming Session Completed in [bold cyan]{elapsed_str}[/]", style="green"),
        Text(f"Total: {stats['total']} | Success: {stats['completed']} | Failed (Likely Photo Posts): {stats['failed']}", style="bold white", justify="center")
    )
    console.print("\n")
    console.print(final_report)

def signal_handler(sig, frame):
    if shutdown_event.is_set(): os._exit(1)
    shutdown_event.set()
    console.print("\n[bold red blink]Abort signal received! Stopping downloads gracefully... (Press Ctrl+C again to force exit)[/]")

signal.signal(signal.SIGINT, signal_handler)

def perform_update():
    import sys, subprocess
    current_v = yt_dlp.version.__version__
    console.print(f"[*] Current Engine Version: [bold cyan]{current_v}[/]")
    
    with console.status("[bold yellow]Contacting PyPI and fetching bleeding-edge yt-dlp core...[/]"):
        try:
            # sys.executable ensures we update the current venv's pip package perfectly
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "-U", "yt-dlp[default]"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            console.print("\n[bold green]✔ Engine completely upgraded! Please restart the script to apply the core upgrades.[/]")
            os._exit(0)
        except Exception as e:
            console.print(f"\n[bold red]✖ Update failed:[/] {e}")

if __name__ == "__main__":
    console.print(Panel("[bold white]TikTok FastBulk: RICH ANALYTICS EDITION v5.0[/]", style="blue"))
    u_input = console.input("[bold]Enter Username (or type 'update' to upgrade engine):[/] ")
    if u_input.strip().lower() == 'update':
        perform_update()
    elif u_input:
        fast_bulk_download(u_input)