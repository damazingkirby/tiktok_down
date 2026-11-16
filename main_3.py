#!/usr/bin/env python3
"""TikTok downloader v11.

Usage:
  <username>[, <username2> ...]   download one or several profiles
  power                           refresh every folder in downloads/ (several at once)
  update                          upgrade yt-dlp (pip)
"""

import concurrent.futures as cf
import glob
import itertools
import json
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

import yt_dlp
from yt_dlp.networking.impersonate import ImpersonateTarget

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    requests = None

from rich.align import Align
from rich.console import Console, Group
from rich.live import Live
from rich.markup import escape
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

# ==================== CONFIG ====================
DOWNLOAD_ROOT = "downloads"
ERROR_LOG = "errors.log"
PROXIES_FILE = "proxies.txt"
COOKIE_CANDIDATES = ("tiktok_cookies.txt", "tiktok_cookie.txt", "cookies.txt")

CONCURRENT_VIDEOS = 20       # worker threads per profile
GLOBAL_MAX_DOWNLOADS = 40    # transfers allowed at once, across all profiles
BULK_PROFILES = 4            # profiles processed at the same time in 'power' mode
IMAGE_WORKERS = 8            # parallel slide downloads per photo post
MAX_RETRIES = 2

SEC_UID_PREFIX = "MS4wLjABAAAA"
RESERVED = frozenset({"power", "update", "source", "cache", "__pycache__"})
USERNAME_RE = re.compile(r"[A-Za-z0-9_.]{1,24}")
ANSI_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-9;?]*[ -/]*[@-~])")
REHYDRATION_RE = re.compile(
    r'<script[^>]+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>', re.DOTALL
)
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tiktok.com/",
}

# FFMPEG_LOCATION can point to ffmpeg.exe or its folder (e.g. C:\yt-dlp) if it's not on PATH
SYSTEM_FFMPEG = os.environ.get("FFMPEG_LOCATION") or shutil.which("ffmpeg")

console = Console()
shutdown_event = threading.Event()
download_sem = threading.BoundedSemaphore(GLOBAL_MAX_DOWNLOADS)
_log_lock = threading.Lock()


def log_error(context, details):
    msg = ANSI_RE.sub("", str(details)).strip()
    if not msg:
        return
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {context} - {msg[:500]}\n"
    with _log_lock:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(line)


def find_cookie_file():
    for name in COOKIE_CANDIDATES:
        if os.path.isfile(name) and os.path.getsize(name) > 50:
            return name
    return None


COOKIE_FILE = find_cookie_file()


def load_cookies(session, path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            parts = line.rstrip("\r\n").split("\t")
            if len(parts) >= 7:
                domain, _, cpath, _, _, name, value = parts[:7]
                session.cookies.set(name, value.strip(), domain=domain, path=cpath)


class ProxyPool:
    """Thread-safe round-robin over proxies.txt (one proxy per line)."""

    def __init__(self, path):
        self._lock = threading.Lock()
        self._cycle = None
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                items = [line.strip() for line in f if line.strip()]
            if items:
                self._cycle = itertools.cycle(items)

    def next(self):
        if self._cycle is None:
            return None
        with self._lock:
            return next(self._cycle)


PROXIES = ProxyPool(PROXIES_FILE)


class Archive:
    """Set of downloaded video IDs. Each new entry is written immediately."""

    def __init__(self, path):
        self.path = path
        self._ids = set()
        self._lock = threading.Lock()
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    parts = line.split()
                    if parts:
                        self._ids.add(parts[-1])

    def __contains__(self, vid):
        return vid in self._ids

    def add(self, vid):
        with self._lock:
            if vid in self._ids:
                return
            self._ids.add(vid)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(f"tiktok {vid}\n")


@dataclass(order=True)
class Task:
    priority: int
    seq: int
    video_id: str = field(compare=False)
    is_slideshow: bool = field(default=False, compare=False)
    retries_left: int = field(default=MAX_RETRIES, compare=False)
    status: str = field(default="Queued", compare=False)
    progress: str = field(default="-", compare=False)
    speed: str = field(default="-", compare=False)
    eta: str = field(default="-", compare=False)
    last_ui: float = field(default=0.0, compare=False)


class _ScoutLogger:
    def __init__(self, engine):
        self.engine = engine

    def debug(self, msg):
        low = msg.lower()
        if "page" in low or "[tiktok:user]" in low:
            self.engine._set_scout(msg.split(":")[-1].strip()[:40])

    def warning(self, msg):
        pass

    def error(self, msg):
        log_error(f"scout @{self.engine.username}", msg)


class _NullLogger:
    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass  # the execution stage logs failures itself


class ProfileEngine:
    def __init__(self, username, deep_scan=False):
        self.username = username
        self.deep_scan = deep_scan
        self.user_dir = os.path.join(DOWNLOAD_ROOT, username)
        os.makedirs(self.user_dir, exist_ok=True)

        self.archive = Archive(os.path.join(self.user_dir, "archive.txt"))
        self._sec_uid_path = os.path.join(self.user_dir, ".sec_uid")
        self.sec_uid = self._read_sec_uid()

        self.queue = queue.PriorityQueue()
        self._seq = itertools.count()
        self.lock = threading.Lock()
        self.seen = set()
        self.active = {}  # future -> Task (main thread writes; render reads under lock)
        self.stats = {"total": 0, "completed": 0, "failed": 0, "skipped": 0,
                      "slideshows": 0, "images": 0}
        self.scout_done = threading.Event()
        self.started = time.time()

        self._tls = threading.local()
        self._opened = []
        self._opened_lock = threading.Lock()

        self.progress = Progress(
            SpinnerColumn(), TextColumn("{task.description}"), BarColumn(),
            TaskProgressColumn(), TimeElapsedColumn(),
        )
        self._bar = self.progress.add_task(f"@{username}", total=None)
        self.full_clean()

    # ---------- small helpers ----------
    def _set_scout(self, msg):
        label = "Power-scan" if self.deep_scan else "Scan"
        self.progress.update(self._bar, description=f"[bold yellow]{label}[/] @{self.username} [dim]{escape(msg)}[/]")

    def _read_sec_uid(self):
        try:
            with open(self._sec_uid_path, encoding="utf-8") as f:
                uid = f.read().strip()
        except OSError:
            return None
        return uid if uid.startswith(SEC_UID_PREFIX) else None

    def _save_sec_uid(self, uid):
        if not uid or not uid.startswith(SEC_UID_PREFIX) or uid == self.sec_uid:
            return
        self.sec_uid = uid
        try:
            with open(self._sec_uid_path, "w", encoding="utf-8") as f:
                f.write(uid)
        except OSError:
            pass

    def full_clean(self):
        cutoff = time.time() - 60
        for pat in ("*.part", "*.ytdl", "*.tmp"):
            for f in glob.glob(os.path.join(glob.escape(self.user_dir), pat)):
                try:
                    if os.path.getmtime(f) < cutoff:
                        os.remove(f)
                except OSError:
                    pass

    def _cleanup_partial(self, vid):
        for f in glob.glob(os.path.join(glob.escape(self.user_dir), f"*{vid}*")):
            if f.endswith((".part", ".ytdl", ".temp", ".tmp")):
                try:
                    os.remove(f)
                except OSError:
                    pass

    # ---------- yt-dlp options and clients ----------
    def _ydl_opts(self, extract, cookies=True):
        opts = {
            "format": "bestvideo+bestaudio/best[vcodec!=none]/best" if SYSTEM_FFMPEG else "best",
            "impersonate": ImpersonateTarget.from_str("chrome"),
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 20 if self.deep_scan else 15,
            "retries": 6 if self.deep_scan else 4,
            "overwrites": True,
            "nopart": False,
            "http_chunk_size": 5 * 1024 * 1024,
        }
        if SYSTEM_FFMPEG:
            opts["ffmpeg_location"] = SYSTEM_FFMPEG
        if cookies and COOKIE_FILE:
            opts["cookiefile"] = COOKIE_FILE
        proxy = PROXIES.next()
        if proxy:
            opts["proxy"] = proxy

        if extract:
            opts["logger"] = _ScoutLogger(self)
            opts["extract_flat"] = "in_playlist" if self.deep_scan else True
            if self.deep_scan:
                opts["lazy_playlist"] = True
                opts["playlist_items"] = "1-99999"
            opts["extractor_args"] = {"tiktok": {
                "web_id": "random", "app_info": "1180",
                "api_hostname": "api16-normal-c-useast1a.tiktokv.com",
            }}
        else:
            opts["logger"] = _NullLogger()
            opts["outtmpl"] = f"{self.user_dir}/[%(id)s] %(title).50s.%(ext)s"
            opts["download_archive"] = self.archive.path
            opts["progress_hooks"] = [self._progress_hook]
            opts["concurrent_fragment_downloads"] = 10 if self.deep_scan else 4
        return opts

    def _downloader(self):
        """One yt-dlp client per worker thread, reused across tasks."""
        ydl = getattr(self._tls, "ydl", None)
        if ydl is None:
            ydl = yt_dlp.YoutubeDL(self._ydl_opts(extract=False))
            self._tls.ydl = ydl
            with self._opened_lock:
                self._opened.append(ydl)
        return ydl

    def _progress_hook(self, d):
        if shutdown_event.is_set():
            raise ValueError("aborted")
        task = getattr(self._tls, "task", None)
        if task is None:
            return
        status = d.get("status")
        if status == "downloading":
            now = time.monotonic()
            if now - task.last_ui < 0.4:
                return
            with self.lock:
                task.last_ui = now
                task.status = "Downloading"
                task.speed = str(d.get("_speed_str", "-")).strip()
                task.progress = str(d.get("_percent_str", "-")).strip()
                task.eta = str(d.get("_eta_str", "-")).strip()
        elif status == "finished":
            with self.lock:
                task.status = "Finalizing"
                task.progress = "100%"
                task.speed = "-"
                task.eta = "-"

    # ---------- HTTP (slideshows) ----------
    def _session(self):
        s = getattr(self._tls, "session", None)
        if s is None:
            if requests is None:
                raise RuntimeError("requests is not installed (pip install requests)")
            s = requests.Session()
            s.headers.update(HTTP_HEADERS)
            retry = Retry(total=2, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
            adapter = HTTPAdapter(pool_connections=8, pool_maxsize=16, max_retries=retry)
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            if COOKIE_FILE:
                try:
                    load_cookies(s, COOKIE_FILE)
                except OSError:
                    pass
            self._tls.session = s
        return s

    def _fetch_item(self, session, vid):
        url = f"https://www.tiktok.com/@{self.username}/video/{vid}"
        r = session.get(url, timeout=15)
        r.raise_for_status()
        m = REHYDRATION_RE.search(r.text)
        if not m:
            raise ValueError("no rehydration data on the post page")
        data = json.loads(m.group(1))
        scope = data.get("__DEFAULT_SCOPE__") or {}
        detail = scope.get("webapp.video-detail") or {}
        info = detail.get("itemInfo") or {}
        item = info.get("itemStruct") or {}
        if not item:
            raise ValueError("empty itemStruct")
        return item

    def _fetch_to_file(self, session, url, dest):
        tmp = dest + ".part"
        try:
            with download_sem:
                with session.get(url, timeout=20, stream=True) as r:
                    r.raise_for_status()
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(1 << 16):
                            if shutdown_event.is_set():
                                raise InterruptedError("aborted")
                            if chunk:
                                f.write(chunk)
            os.replace(tmp, dest)
            return True
        except Exception:
            try:
                os.remove(tmp)
            except OSError:
                pass
            return False

    def _download_slideshow(self, task):
        session = self._session()
        item = self._fetch_item(session, task.video_id)
        images = (item.get("imagePost") or {}).get("images") or []
        if not images:
            return 0

        def fetch_slide(pair):
            idx, img = pair
            dest = os.path.join(self.user_dir, f"[{task.video_id}] slide_{idx + 1:02d}.jpg")
            if os.path.exists(dest):
                return 1
            for url in ((img.get("imageURL") or {}).get("urlList")) or []:
                if self._fetch_to_file(session, url, dest):
                    return 1
            return 0

        with cf.ThreadPoolExecutor(max_workers=min(IMAGE_WORKERS, len(images))) as pool:
            saved = sum(pool.map(fetch_slide, enumerate(images)))

        if saved:
            music = (item.get("music") or {}).get("playUrl")
            dest = os.path.join(self.user_dir, f"[{task.video_id}] audio.mp3")
            if music and not os.path.exists(dest):
                self._fetch_to_file(session, music, dest)
            with self.lock:
                self.stats["images"] += saved
        return saved

    # ---------- discovery ----------
    def _enqueue(self, entry):
        if not entry:
            return
        vid = entry.get("id")
        if not vid:
            return
        if entry.get("channel_id"):
            self._save_sec_uid(entry["channel_id"])
        is_slide = entry.get("vcodec") == "none" or entry.get("duration") == 0

        with self.lock:
            if vid in self.seen:
                return
            self.seen.add(vid)
            if vid in self.archive:
                self.stats["skipped"] += 1
                return
            self.stats["total"] += 1
            if is_slide:
                self.stats["slideshows"] += 1
            total = self.stats["total"]
            task = Task(priority=1, seq=next(self._seq), video_id=vid, is_slideshow=is_slide,
                        retries_left=MAX_RETRIES)
        self.queue.put(task)
        self.progress.update(self._bar, total=total)

    def _consume(self, info):
        """Returns True if the page yielded at least one entry (even if all were already saved)."""
        if not info:
            return False
        if info.get("channel_id"):
            self._save_sec_uid(info["channel_id"])
        got = False
        for entry in (info.get("entries") or []):
            if shutdown_event.is_set():
                break
            got = True
            self._enqueue(entry)
        return got

    def _scout(self):
        try:
            u = self.username
            stages = []
            if self.sec_uid:
                stages.append(("secUid", f"tiktokuser:{self.sec_uid}", self.deep_scan, True))
            stages.append(("web", f"https://www.tiktok.com/@{u}", self.deep_scan, True))
            stages.append(("anonymous", f"https://www.tiktok.com/@{u}", False, False))

            got = False
            for label, url, process, cookies in stages:
                if got or shutdown_event.is_set():
                    break
                self._set_scout(f"{label} scan...")
                try:
                    with yt_dlp.YoutubeDL(self._ydl_opts(extract=True, cookies=cookies)) as ydl:
                        info = ydl.extract_info(url, download=False, process=process)
                        got = self._consume(info)  # inside the block: lazy playlists need the client alive
                except Exception as e:
                    log_error(f"scout {label} @{u}", e)

            if got:
                self._set_scout("ready")
            else:
                self._set_scout("no entries (private, empty or blocked)")
        except Exception as e:
            log_error(f"scout crash @{self.username}", e)
        finally:
            self.scout_done.set()

    # ---------- execution ----------
    def _run_task(self, task):
        """Returns True on success. Never raises."""
        if shutdown_event.is_set():
            return False
        self._tls.task = task
        url = f"https://www.tiktok.com/@{self.username}/video/{task.video_id}"
        try:
            if task.is_slideshow:
                with self.lock:
                    task.status = "Slideshow"
                if self._download_slideshow(task):
                    self.archive.add(task.video_id)
                    return True
                # not a real photo post after all: fall through to yt-dlp

            with self.lock:
                task.status = "Video"
            with download_sem:
                self._downloader().download([url])
            self.archive.add(task.video_id)
            return True
        except Exception as e:
            if not shutdown_event.is_set():
                log_error(f"video {task.video_id} @{self.username}", e)
            self._tls.ydl = None  # next attempt uses a fresh client (stale token / 403)
            self._cleanup_partial(task.video_id)
            return False
        finally:
            self._tls.task = None

    def _settle(self, task, ok):
        if ok:
            with self.lock:
                self.stats["completed"] += 1
            self.progress.advance(self._bar)
        elif shutdown_event.is_set():
            return  # interrupted, not a real failure
        elif task.retries_left > 0:
            with self.lock:
                task.retries_left -= 1
                task.priority = 0  # retries jump the queue
                task.status = "Retry"
                task.progress = "0%"
                task.speed = "-"
                task.eta = "-"
            self.queue.put(task)
        else:
            with self.lock:
                self.stats["failed"] += 1
            self.progress.advance(self._bar)

    def _drain(self, on_tick=None):
        pool = cf.ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS, thread_name_prefix=f"dl-{self.username}")
        try:
            while not shutdown_event.is_set():
                while len(self.active) < CONCURRENT_VIDEOS:
                    try:
                        task = self.queue.get_nowait()
                    except queue.Empty:
                        break
                    with self.lock:
                        self.active[pool.submit(self._run_task, task)] = task

                if not self.active:
                    if self.scout_done.is_set() and self.queue.empty():
                        break
                    time.sleep(0.1)
                else:
                    done, _ = cf.wait(list(self.active), timeout=0.25, return_when=cf.FIRST_COMPLETED)
                    for fut in done:
                        with self.lock:
                            task = self.active.pop(fut)
                        try:
                            ok = bool(fut.result())
                        except Exception:
                            ok = False
                        self._settle(task, ok)

                if on_tick:
                    on_tick()
        finally:
            pool.shutdown(wait=True, cancel_futures=True)

    # ---------- UI ----------
    def _render(self):
        table = Table(title="[bold magenta]Live pipeline[/]", expand=True, border_style="cyan")
        table.add_column("ID", style="dim cyan", width=16)
        table.add_column("Type", width=8)
        table.add_column("Status", style="yellow")
        table.add_column("Progress", justify="right")
        table.add_column("Speed", style="blue", justify="right")
        table.add_column("ETA", style="red", justify="right")
        with self.lock:
            rows = [(t.video_id, t.is_slideshow, t.status, t.progress, t.speed, t.eta)
                    for t in list(self.active.values())[:15]]
            s = dict(self.stats)
        for vid, slide, status, prog, speed, eta in rows:
            table.add_row(vid[:16], "📸" if slide else "🎬", status, prog, speed, eta)
        elapsed = time.strftime("%H:%M:%S", time.gmtime(time.time() - self.started))
        line = (f"Total {s['total']} | ✅ {s['completed']} | ⏭ {s['skipped']} | "
                f"❌ {s['failed']} | 📸 {s['images']} imgs | ⏱ {elapsed}")
        return Group(
            Panel(self.progress, title=f"@{self.username}", border_style="blue"),
            table,
            Panel(Align.center(line), style="bold white on black", border_style="green"),
        )

    # ---------- entry points ----------
    def run(self, live_ui=False):
        threading.Thread(target=self._scout, daemon=True, name=f"scout-{self.username}").start()
        try:
            if live_ui:
                with Live(self._render(), console=console, refresh_per_second=4) as live:
                    self._drain(on_tick=lambda: live.update(self._render()))
            else:
                self._drain()
        finally:
            self._close()
        with self.lock:
            return dict(self.stats)

    def _close(self):
        with self._opened_lock:
            for ydl in self._opened:
                try:
                    ydl.close()
                except Exception:
                    pass
            self._opened.clear()
        self.full_clean()


# ==================== COMMANDS ====================
def parse_usernames(raw):
    out, seen = [], set()
    for tok in re.split(r"[\s,]+", raw or ""):
        name = tok.strip().lstrip("@")
        if name and USERNAME_RE.fullmatch(name) and name.lower() not in RESERVED and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def bulk_update():
    if not os.path.isdir(DOWNLOAD_ROOT):
        console.print(f"[red]No '{DOWNLOAD_ROOT}' folder.[/]")
        return
    names = [d for d in os.listdir(DOWNLOAD_ROOT) if os.path.isdir(os.path.join(DOWNLOAD_ROOT, d))]
    users = parse_usernames(" ".join(names))
    if not users:
        console.print("[yellow]No profile folders found.[/]")
        return

    console.print(Panel(f"{len(users)} profiles, {BULK_PROFILES} at a time",
                        title="Power mode", border_style="green"))

    def job(user):
        if shutdown_event.is_set():
            return user, None
        try:
            return user, ProfileEngine(user, deep_scan=True).run(live_ui=False)
        except Exception as e:
            log_error(f"power @{user}", e)
            return user, {"error": str(e)}

    results = {}
    with cf.ThreadPoolExecutor(max_workers=BULK_PROFILES) as pool:
        pending = {pool.submit(job, u) for u in users}
        while pending:
            done, pending = cf.wait(pending, timeout=1, return_when=cf.FIRST_COMPLETED)
            for fut in done:
                if fut.cancelled():
                    continue
                user, stats = fut.result()
                if stats is None:
                    continue
                results[user] = stats
                if "error" in stats:
                    console.print(f"[red]✖ @{user}: {escape(stats['error'])}[/]")
                else:
                    console.print(f"[green]✔ @{user}[/] +{stats['completed']} new, "
                                  f"{stats['failed']} failed, {stats['images']} images")
            if shutdown_event.is_set():
                for fut in pending:
                    fut.cancel()

    table = Table(title="[bold green]Power summary[/]", expand=True, border_style="green")
    table.add_column("Profile", style="cyan")
    table.add_column("New", justify="right", style="bold green")
    table.add_column("Images", justify="right", style="magenta")
    table.add_column("Failed", justify="right", style="red")
    total_new = 0
    for u in users:
        s = results.get(u)
        if s is None:
            table.add_row(f"@{u}", "not run", "-", "-")
        elif "error" in s:
            table.add_row(f"@{u}", "ERROR", "-", "-")
        else:
            table.add_row(f"@{u}", str(s["completed"]), str(s["images"]), str(s["failed"]))
            total_new += s["completed"]
    console.print(table)
    console.print(f"[bold green]Total new items: {total_new}[/]")


def perform_update():
    console.print(f"Current yt-dlp: [cyan]{yt_dlp.version.__version__}[/]")
    with console.status("Upgrading yt-dlp..."):
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-U", "yt-dlp"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            console.print(f"[red]Update failed: {e}[/]")
            return
    console.print("[green]Updated. Restart the script.[/]")


def _on_sigint(signum, frame):
    if shutdown_event.is_set():
        os._exit(1)
    shutdown_event.set()
    console.print("\n[bold red]Stopping... (Ctrl+C again to force quit)[/]")


def main():
    signal.signal(signal.SIGINT, _on_sigint)
    console.print(Panel("[bold white]TikTok downloader v11[/]", style="blue"))
    if requests is None:
        console.print("[yellow]'requests' missing: photo posts will fail (pip install requests)[/]")
    if SYSTEM_FFMPEG is None:
        console.print("[yellow]ffmpeg not found: set FFMPEG_LOCATION or add it to PATH[/]")

    choice = console.input("[bold]Username(s), 'power' or 'update':[/] ").strip()
    low = choice.lower()
    if low == "update":
        perform_update()
        return
    if low == "power":
        bulk_update()
        return

    users = parse_usernames(choice)
    if not users:
        console.print("[yellow]No valid username.[/]")
        return
    for i, user in enumerate(users, 1):
        if shutdown_event.is_set():
            break
        console.print(f"\n[bold magenta]━━ {i}/{len(users)} @{user} ━━[/]")
        stats = ProfileEngine(user, deep_scan=False).run(live_ui=True)
        console.print(f"[green]Done[/] {stats['completed']} new, {stats['failed']} failed, "
                      f"{stats['skipped']} already saved")
        if i < len(users) and not shutdown_event.is_set():
            time.sleep(1)


if __name__ == "__main__":
    main()