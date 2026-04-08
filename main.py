import yt_dlp
import os
import signal
import time
import glob
import concurrent.futures
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
CONCURRENT_VIDEOS = 10
COOKIE_FILE = 'tiktok_cookies.txt' # Optional cookie file
CHROME_TARGET = ImpersonateTarget.from_str('chrome')
shutdown_event = Event()
console = Console()

# Global state for UI
stats = {
    "total": 0, 
    "completed": 0, 
    "failed": 0,
    "start_time": 0,
}
worker_status = {} # {vid: {'status': '', 'speed': '', 'percent': '', 'eta': ''}}
status_lock = Lock()

overall_progress = Progress(
    SpinnerColumn(),
    TextColumn("[progress.description]{task.description}"),
    BarColumn(),
    TaskProgressColumn(),
    TimeElapsedColumn(),
)
main_task = None

def progress_hook(d):
    if shutdown_event.is_set():
        raise ValueError("Download Cancelled by User")
        
    video_id = d.get('info_dict', {}).get('id', 'Unknown')
    if d['status'] == 'downloading':
        speed = d.get('_speed_str', 'N/A')
        percent = d.get('_percent_str', '  0%')
        eta = d.get('_eta_str', 'N/A')
        with status_lock:
            if video_id not in worker_status:
                worker_status[video_id] = {}
            worker_status[video_id].update({
                'status': 'Downloading',
                'speed': speed,
                'percent': percent,
                'eta': eta
            })
    elif d['status'] == 'finished':
        with status_lock:
            if video_id in worker_status:
                worker_status[video_id].update({
                    'status': 'Merging/Finalizing',
                    'speed': '-',
                    'percent': '100%',
                    'eta': '-'
                })

def get_ydl_opts(user_dir, is_extractor=False):
    opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'merge_output_format': 'mp4',
        'impersonate': CHROME_TARGET,
        'quiet': True,
        'no_warnings': True,
        'ffmpeg_location': 'C:/yt-dlp',
        'extractor_args': {'tiktok': {'web_id': 'random', 'app_info': '1180'}},
        'socket_timeout': 30,
        'retries': 10,
        'nopart': True,
        'overwrites': True,
        'concurrent_fragment_downloads': 6, # Massive speedup
        'progress_hooks': [progress_hook],
    }
    # Optional Cookie integration
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

def clean_stray_files(user_dir):
    garbage = ['*.mp3', '*.m4a', '*.webm', '*.tmp', '*.part', '*.ytdl', '*.f*']
    for ext in garbage:
        for f in glob.glob(os.path.join(user_dir, ext)):
            try: os.remove(f)
            except: pass

def download_worker(url, user_dir):
    if shutdown_event.is_set(): return False
    video_id = url.split('/')[-1]
    
    with status_lock:
        worker_status[video_id] = {
            'status': 'Initializing',
            'speed': '-',
            'percent': '0%',
            'eta': '-'
        }
        
    try:
        with yt_dlp.YoutubeDL(get_ydl_opts(user_dir)) as ydl:
            ydl.download([url])
        clean_stray_files(user_dir)
        return True
    except:
        clean_stray_files(user_dir)
        return False

def generate_dashboard():
    """Builds the comprehensive Rich UI Layout"""
    # Active Workers Table
    table = Table(title="[bold magenta]Active Worker Analytics[/]", expand=True, border_style="cyan")
    table.add_column("Video ID", style="dim cyan", width=20)
    table.add_column("Status", style="bold yellow")
    table.add_column("Progress", style="bold green", justify="right")
    table.add_column("Speed", style="bold blue", justify="right")
    table.add_column("ETA", style="bold red", justify="right")

    with status_lock:
        # Show recent active workers up to limit
        active_items = list(worker_status.items())[-CONCURRENT_VIDEOS:]
        for vid, data in active_items:
            table.add_row(
                vid, 
                data.get('status', 'Unknown'), 
                data.get('percent', '0%'), 
                data.get('speed', '-'), 
                data.get('eta', '-')
            )
        
        # Fill empty rows
        for _ in range(CONCURRENT_VIDEOS - len(active_items)):
            table.add_row("-", "[dim]Idle[/]", "-", "-", "-")

    # Overall Summary Panel
    elapsed = time.time() - stats["start_time"] if stats["start_time"] else 0
    elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    
    summary_text = (
        f"[bold cyan]Total Videos:[/] {stats['total']}  |  "
        f"[bold green]Success:[/] {stats['completed']}  |  "
        f"[bold red]Failed (Photo Posts):[/] {stats['failed']}  |  "
        f"[bold yellow]Time Elapsed:[/] {elapsed_str}"
    )
    
    summary_panel = Panel(Align.center(summary_text), style="bold white on black", border_style="green")
    
    group = Group(
        Panel(overall_progress, title="[bold blue]Overall Job Progress[/]", border_style="blue"),
        table,
        summary_panel
    )
    return group

def fast_bulk_download(input_name):
    global main_task
    username = input_name.strip().replace("@", "")
    user_dir = os.path.join("downloads", username)
    if not os.path.exists(user_dir): os.makedirs(user_dir)
    
    clean_stray_files(user_dir)
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

    # Live Display logic with non-blocking updates
    with Live(generate_dashboard(), refresh_per_second=10) as live:
        with ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
            future_to_url = {executor.submit(download_worker, url, user_dir): url for url in video_urls}
            not_done = set(future_to_url.keys())
            
            while not_done and not shutdown_event.is_set():
                # Refresh UI every cycle
                live.update(generate_dashboard())
                
                # Check for completed futures with a small timeout to allow UI update
                done, not_done = concurrent.futures.wait(not_done, timeout=0.25)
                
                for future in done:
                    url = future_to_url[future]
                    try:
                        success = future.result()
                    except Exception:
                        success = False
                        
                    vid_id = url.split('/')[-1]
                    
                    if success: stats["completed"] += 1
                    else: stats["failed"] += 1
                    
                    # Update Progress and Cleanup Status
                    overall_progress.update(main_task, advance=1)
                    with status_lock:
                        if vid_id in worker_status: del worker_status[vid_id]
            
            if shutdown_event.is_set():
                for future in not_done:
                    future.cancel()

    clean_stray_files(user_dir)
    elapsed = time.time() - stats["start_time"]
    elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    
    final_report = Group(
        Panel(f"Download Session Completed in [bold cyan]{elapsed_str}[/]", style="green"),
        Text(f"Total: {stats['total']} | Success: {stats['completed']} | Failed (Likely Photo Posts): {stats['failed']}", style="bold white", justify="center")
    )
    console.print("\n")
    console.print(final_report)

def signal_handler(sig, frame):
    if shutdown_event.is_set():
        os._exit(1)
    shutdown_event.set()
    console.print("\n[bold red blink]Abort signal received! Stopping downloads gracefully... (Press Ctrl+C again to force exit)[/]")

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    console.print(Panel("[bold white]TikTok FastBulk: RICH ANALYTICS EDITION v3.0[/]", style="blue"))
    u_input = console.input("[bold]Enter Username:[/] ")
    if u_input:
        fast_bulk_download(u_input)