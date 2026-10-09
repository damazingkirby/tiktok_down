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
import json
from dataclasses import dataclass, field
from yt_dlp.networking.impersonate import ImpersonateTarget

try:
    import requests
except ImportError:
    requests = None

# Rich UI Imports
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.table import Table
from rich.align import Align
from rich.text import Text

# --- GLOBALS & CONFIG ---
CONCURRENT_VIDEOS = 20                 # per-profile worker pool size
GLOBAL_MAX_CONCURRENT_DOWNLOADS = 40    # hard cap on simultaneous network downloads across ALL profiles at once
BULK_CONCURRENT_PROFILES = 4            # how many profiles 'power' mode churns through in parallel
IMAGE_WORKERS = 8                       # parallel image fetches within a single slideshow post
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

# Matches TikTok's client-hydration JSON blob, which is the only place the
# per-slide image URLs (imagePost.images[].imageURL.urlList) actually live.
# yt-dlp's own TikTok extractor never surfaces this - it only returns the
# background audio track for photo/slideshow posts - so slideshows have to
# be fetched straight from the page ourselves.
UNIVERSAL_DATA_RE = re.compile(
    r'<script[^>]+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
    re.DOTALL
)

IMAGE_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

# Reserved non-username folders to skip during bulk updates
RESERVED_FOLDERS = {
    'power', 'update', 'source', 'cache', '__pycache__',
    'test_clean', 'test_embed', 'test_slides', 'test_lazy'
}

shutdown_event = threading.Event()
console = Console()
status_lock = threading.Lock()
thread_local = threading.local()

# Global cap on concurrent network downloads (video OR image), shared across
# every profile/thread so bulk mode can run many profiles in parallel without
# opening hundreds of simultaneous connections.
download_semaphore = threading.BoundedSemaphore(GLOBAL_MAX_CONCURRENT_DOWNLOADS)

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


# --- SLIDESHOW / PHOTO-POST HANDLING ---
# yt-dlp treats TikTok photo/slideshow posts as audio-only clips and never
# extracts the individual slide images (there's no upstream extractor support
# for it - see yt-dlp issues #9990 / #10034). The actual per-slide image URLs
# only exist in the page's __UNIVERSAL_DATA_FOR_REHYDRATION__ JSON blob under
# itemStruct.imagePost.images[].imageURL.urlList. So for slideshows we skip
# yt-dlp's downloader entirely and pull the images straight from the page.

def get_requests_session(cookie_file=None):
    if requests is None:
        raise RuntimeError("The 'requests' library is required for slideshow/photo downloads (pip install requests)")
    session = getattr(thread_local, 'session', None)
    if session is None:
        session = requests.Session()
        session.headers.update(IMAGE_REQUEST_HEADERS)
        thread_local.session = session
        thread_local.loaded_cookie_file = None
    if cookie_file and getattr(thread_local, 'loaded_cookie_file', None) != cookie_file:
        load_cookies_into_session(session, cookie_file)
        thread_local.loaded_cookie_file = cookie_file
    return session


def load_cookies_into_session(session, cookie_file):
    """Parses a Netscape-format cookie file into a requests session."""
    try:
        with open(cookie_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split('\t')
                if len(parts) >= 7:
                    domain, _, path, _secure, _expires, name, value = parts[:7]
                    session.cookies.set(name, value, domain=domain, path=path)
    except Exception:
        pass


def fetch_item_struct(session, video_id, username):
    """Fetches the raw TikTok item data (including imagePost) for a single post."""
    url = f"https://www.tiktok.com/@{username}/video/{video_id}"
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    match = UNIVERSAL_DATA_RE.search(resp.text)
    if not match:
        raise ValueError("Could not locate rehydration data on the post page")
    data = json.loads(match.group(1))
    scope = data.get('__DEFAULT_SCOPE__', {}) or {}
    item = (
        scope.get('webapp.video-detail', {}) or {}
    ).get('itemInfo', {}).get('itemStruct', {}) if scope else {}
    if not item:
        raise ValueError("No item data found (post may be private, deleted, or region-locked)")
    return item


def download_binary(session, url, dest_path):
    tmp_path = dest_path + '.part'
    with download_semaphore:
        r = session.get(url, timeout=25, stream=True)
        r.raise_for_status()
        with open(tmp_path, 'wb') as f:
            for chunk in r.iter_content(1 << 16):
                if chunk:
                    f.write(chunk)
        os.replace(tmp_path, dest_path)


def download_slideshow(engine, task):
    """Downloads every full-resolution slide image (plus the background audio,
    best-effort) for a TikTok photo/slideshow post. Returns count of images saved."""
    cookie_file = get_available_cookie_file()
    session = get_requests_session(cookie_file)

    item = fetch_item_struct(session, task.video_id, engine.username)
    images = ((item.get('imagePost') or {}).get('images')) or []
    if not images:
        raise ValueError("No images found in imagePost - this post may not actually be a slideshow")

    saved = 0
    errors = []
    save_lock = threading.Lock()

    def _dl_one(idx, img):
        nonlocal saved
        url_list = ((img.get('imageURL') or {}).get('urlList')) or []
        if not url_list:
            return
        dest = os.path.join(engine.user_dir, f"[{task.video_id}] slide_{idx + 1:02d}.jpg")
        if os.path.exists(dest):
            with save_lock:
                saved += 1
            return
        last_exc = None
        dl_session = get_requests_session(cookie_file)
        for candidate_url in url_list:
            try:
                download_binary(dl_session, candidate_url, dest)
                with save_lock:
                    saved += 1
                return
            except Exception as e:
                last_exc = e
                continue
        if last_exc:
            with save_lock:
                errors.append(str(last_exc))

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(IMAGE_WORKERS, len(images))) as pool:
        list(pool.map(lambda pair: _dl_one(pair[0], pair[1]), enumerate(images)))

    # Best-effort: also grab the background music track that came with the slideshow
    try:
        music_url = (item.get('music') or {}).get('playUrl')
        if music_url:
            dest = os.path.join(engine.user_dir, f"[{task.video_id}] audio.mp3")
            if not os.path.exists(dest):
                download_binary(get_requests_session(cookie_file), music_url, dest)
    except Exception:
        pass

    if saved == 0:
        raise ValueError(f"Failed to save any slide images: {'; '.join(errors[:3]) or 'unknown error'}")

    return saved


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
    last_progress_update: float = field(default=0.0, compare=False)

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
    """Refined Engine (v9.0): Universal Format Handler & Adaptive Profile Scouter, with real photo-post support."""

    def __init__(self, username, deep_scan=False, headless=False):
        self.username = username.strip().replace("@", "")
        self.deep_scan = deep_scan
        self.headless = headless  # True when driven by parallel bulk mode: no per-engine Live UI
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
            "images_saved": 0,
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

    def _append_archive(self, video_id):
        """Manually appends to the yt-dlp-style archive file for items we downloaded
        outside of yt-dlp (i.e. slideshows), so they aren't re-scanned next time."""
        try:
            with status_lock:
                with open(self.archive_file, 'a', encoding='utf-8') as f:
                    f.write(f"tiktok {video_id}\n")
        except Exception:
            pass

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
                now = time.monotonic()
                if now - task.last_progress_update < 0.25:
                    return
                with status_lock:
                    task.last_progress_update = now
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
            'concurrent_fragment_downloads': 10,
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
        """Execution Stage: Downloads videos, or fetches slideshow images directly,
        with automatic retry and stream refresh. Bounded by the global download semaphore
        so parallel profiles (bulk mode) can't overrun the network."""
        if shutdown_event.is_set():
            return (False, task)

        thread_local.current_task = task
        with status_lock:
            task.status = "Initializing"

        # --- Photo / slideshow posts: fetched directly, never through yt-dlp ---
        if task.is_slideshow:
            with status_lock:
                task.status = "Fetching Images"
            try:
                n_images = download_slideshow(self, task)
                with status_lock:
                    task.status = "Finalizing"
                    task.progress = "100%"
                    task.speed = "-"
                    task.eta = "-"
                    self.archived_ids.add(task.video_id)
                    self.stats['images_saved'] += n_images
                self._append_archive(task.video_id)
                return (True, task)
            except Exception as primary_exc:
                log_error(f"Slideshow Failed for {task.video_id}", str(primary_exc))
                return (False, task)

        # --- Regular videos: normal yt-dlp path ---
        # Local YDL cache per worker thread, scoped to this engine.
        ydl_owner = (self.username, self.archive_file)
        if getattr(thread_local, 'ydl_owner', None) != ydl_owner:
            thread_local.ydl = yt_dlp.YoutubeDL(self.get_ydl_opts(is_extractor=False))
            thread_local.ydl_owner = ydl_owner

        try:
            with download_semaphore:
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
                    thread_local.ydl_owner = ydl_owner
                    with download_semaphore:
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
        table = Table(title="[bold magenta]Live Pipeline Window (v9.0)[/]", expand=True, border_style="cyan")
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
            f"[bold magenta]Slideshows:[/] {self.stats['slideshows']} ({self.stats['images_saved']} imgs)  |  "
            f"[bold yellow]Uptime:[/] {elapsed_str}"
        )
        summary_panel = Panel(Align.center(summary), style="bold white on black", border_style="green")

        return Group(
            Panel(self.progress_bar, title="[bold blue]TikTok Pipeline Controller[/]", border_style="blue"),
            table,
            summary_panel
        )

    def _drain_queue(self, live=None):
        """Core controller loop shared by the interactive and headless pipelines.
        Feeds workers, resolves finished futures, requeues failures, updates stats.
        If `live` is given, refreshes the Rich UI each pass; otherwise runs silently."""
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
            while not shutdown_event.is_set():
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
                        time.sleep(0.2 if live else 0.05)
                        if self.stats['total'] > 0:
                            self.progress_bar.update(self.main_task_id, completed=self.stats['total'])
                        if live:
                            live.update(self.generate_ui())
                        break
                    else:
                        if live:
                            live.update(self.generate_ui())
                        else:
                            time.sleep(0.05)
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
                        self.active_tasks.pop(future, None)

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

                if live:
                    live.update(self.generate_ui())

            if shutdown_event.is_set():
                for f in self.active_tasks.keys():
                    f.cancel()

    def run_pipeline(self):
        """Controller Stage: Coordinates scout and execution workers with a Rich live terminal UI.
        Use for single/interactive runs. For parallel bulk mode, use run_pipeline_headless()."""
        t_scout = threading.Thread(target=self.scout_routine, daemon=True)
        t_scout.start()

        with Live(self.generate_ui(), refresh_per_second=12) as live:
            self._drain_queue(live=live)

        self.full_clean()

        elapsed = time.time() - self.stats["start_time"]
        final_report = Group(
            Panel(f"Pipeline Completed in [bold cyan]{time.strftime('%H:%M:%S', time.gmtime(elapsed))}[/]", style="green"),
            Text(
                f"Target: {self.stats['total']} | Already Saved: {self.stats['skipped']} | Executed: {self.stats['completed']} | "
                f"Slideshows: {self.stats['slideshows']} ({self.stats['images_saved']} imgs) | Failed: {self.stats['failed']}",
                style="bold white",
                justify="center"
            )
        )
        console.print("\n")
        console.print(final_report)

    def run_pipeline_headless(self):
        """Same controller loop as run_pipeline(), but with no Rich Live output - safe to
        run from multiple threads at once (used by the parallel bulk updater)."""
        t_scout = threading.Thread(target=self.scout_routine, daemon=True)
        t_scout.start()
        self._drain_queue(live=None)
        self.full_clean()
        return self.stats

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
    """Scans valid profile directories in downloads/ and refreshes them, running
    BULK_CONCURRENT_PROFILES profiles at once (bounded by the global download
    semaphore) instead of one full profile at a time."""
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

    workers = min(BULK_CONCURRENT_PROFILES, len(usernames))
    console.print(Panel(
        f"[bold cyan]Bulk Update Core Activated: {len(usernames)} profiles queued, "
        f"running {workers}x in parallel (global cap: {GLOBAL_MAX_CONCURRENT_DOWNLOADS} concurrent downloads)[/]",
        title="TikTok Bulk Updater (v9.0 Parallel)",
        border_style="green"
    ))

    engines_lock = threading.Lock()
    active_engines = {}   # username -> engine, only while running
    results = {}          # username -> stats, once finished
    results_lock = threading.Lock()

    def worker(user):
        engine = TikTokEngine(user, deep_scan=True, headless=True)
        with engines_lock:
            active_engines[user] = engine
        try:
            stats = engine.run_pipeline_headless()
        except Exception as e:
            stats = {"completed": 0, "failed": 0, "total": 0, "images_saved": 0, "error": str(e)}
        with engines_lock:
            active_engines.pop(user, None)
        with results_lock:
            results[user] = stats
        return user, stats

    def render_table():
        table = Table(
            title=f"[bold magenta]Bulk Pipeline - {len(usernames)} profiles, {workers}x parallel[/]",
            expand=True, border_style="cyan"
        )
        table.add_column("Profile", style="cyan")
        table.add_column("Status", style="yellow")
        table.add_column("Found", justify="right")
        table.add_column("Done", justify="right", style="green")
        table.add_column("Failed", justify="right", style="red")
        table.add_column("Slide Imgs", justify="right", style="magenta")

        with engines_lock:
            running_users = list(active_engines.items())
        for user, eng in running_users:
            s = eng.stats
            table.add_row(f"@{user}", "Running", str(s['total']), str(s['completed']), str(s['failed']), str(s['images_saved']))

        with results_lock:
            finished_count = len(results)

        summary = f"[bold cyan]Profiles completed: {finished_count}/{len(usernames)}[/]"
        return Group(table, Panel(Align.center(summary), style="bold white on black", border_style="green"))

    with Live(render_table(), refresh_per_second=6) as live:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(worker, u) for u in usernames]
            while True:
                live.update(render_table())
                if all(f.done() for f in futures):
                    break
                if shutdown_event.is_set():
                    break
                time.sleep(0.2)
            for f in futures:
                try:
                    f.result(timeout=0.1)
                except Exception:
                    pass
        live.update(render_table())

    table = Table(title="[bold green]Bulk Update Final Summary[/]", expand=True, border_style="green")
    table.add_column("Profile (Username)", style="cyan")
    table.add_column("New Items Processed", justify="right", style="bold green")
    table.add_column("Slide Images", justify="right", style="bold magenta")
    table.add_column("Failed", justify="right", style="bold red")

    total_new_vids = 0
    total_images = 0
    for user in usernames:
        stats = results.get(user)
        if stats is None:
            table.add_row(f"@{user}", "INCOMPLETE", "0", "0")
            continue
        if "error" in stats:
            table.add_row(f"@{user}", "ERROR", "0", "0")
            continue
        new = stats.get('completed', 0)
        imgs = stats.get('images_saved', 0)
        failed = stats.get('failed', 0)
        table.add_row(f"@{user}", str(new), str(imgs), str(failed))
        total_new_vids += new
        total_images += imgs

    console.print("\n")
    console.print(table)
    console.print(Panel(
        f"[bold green]Bulk update complete! Total items: {total_new_vids}  |  Total slide images: {total_images}[/]",
        border_style="bold green"
    ))

if __name__ == "__main__":
    console.print(Panel(
        "[bold white]TikTok FastBulk Pipeline (v9.0 - Parallel Bulk + Real Photo/Slideshow Support)[/]\n"
        "[dim]Universal Video & Photo/Slideshow Support | Smart secUid Cache | Zero Format Error[/]",
        style="blue"
    ))
    if requests is None:
        console.print(Panel(
            "[bold yellow]Warning: the 'requests' library isn't installed - slideshow/photo posts will fail to download.\n"
            "Run: pip install requests[/]",
            style="yellow"
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
