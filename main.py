import yt_dlp
import os
import signal
import logging
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, Lock
from yt_dlp.networking.impersonate import ImpersonateTarget

# --- CONFIGURATION ---
CONCURRENT_VIDEOS = 5
CHROME_TARGET = ImpersonateTarget.from_str('chrome')
shutdown_event = Event()
print_lock = Lock()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S')
logger = logging.getLogger("TikTokTurbo")

stats = {"total": 0, "completed": 0}

def get_ydl_opts(user_dir, is_extractor=False):
    opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'merge_output_format': 'mp4',
        'impersonate': CHROME_TARGET,
        'quiet': True,
        'no_warnings': True,
        'ffmpeg_location': 'C:/yt-dlp',
        'cookiefile': 'tiktok_cookies.txt',
        'extractor_args': {'tiktok': {'web_id': 'random', 'app_info': '1180'}},
        'nopart': False,
        
        # --- BOSS MODE STABILITY FIXES ---
        'socket_timeout': 30,         # Don't let workers hang
        'retries': 10,                # Robust retry logic
        'fragment_retries': 10,       # Retry fragments if they fail
        'retry_sleep_functions': {'http': lambda n: 5 * (n + 1)}, # Incremental backoff
        # This force-disables HTTP/2 to stop the "Stream 1 not closed" errors
        'connector_args': {'force_no_http2': True}, 
    }
    
    if is_extractor:
        opts['extract_flat'] = True
    else:
        opts.update({
            'outtmpl': f'{user_dir}/%(title).50s [%(id)s].%(ext)s',
            'download_archive': os.path.join(user_dir, 'archive.txt'),
            'concurrent_fragment_downloads': 5,
            'keepvideo': False,
        })
    return opts

def download_worker(url, user_dir):
    if shutdown_event.is_set():
        return False
    try:
        # We wrap in a simple internal loop for "Boss Level" resilience
        success = False
        attempts = 0
        while not success and attempts < 3:
            try:
                with yt_dlp.YoutubeDL(get_ydl_opts(user_dir)) as ydl:
                    ydl.download([url])
                success = True
            except Exception as e:
                attempts += 1
                if "HTTP/2" in str(e) or "stream" in str(e):
                    time.sleep(2) # Brief pause before retry
                    continue
                break # If it's a different error, stop

        # Cleanup stray mp3 files (your signature fix)
        for file in os.listdir(user_dir):
            if file.endswith(".mp3"):
                try:
                    os.remove(os.path.join(user_dir, file))
                except: pass
        return True
    except Exception as e:
        if not shutdown_event.is_set():
            # Only log actual failures, ignore the noise
            return False

def signal_handler(sig, frame):
    logger.warning("\n[!] INTERRUPT: Shutting down.")
    shutdown_event.set()
    os._exit(1)

signal.signal(signal.SIGINT, signal_handler)

def fast_bulk_download(input_name):
    # Ensure ANSI support for Windows (just in case)
    os.system('')
    
    username = input_name.strip().replace("@", "")
    user_dir = os.path.join("downloads", username)
    if not os.path.exists(user_dir):
        os.makedirs(user_dir)

    logger.info(f"Gathering video list for @{username}...")
    video_urls = []
    try:
        with yt_dlp.YoutubeDL(get_ydl_opts(user_dir, is_extractor=True)) as ydl:
            info = ydl.extract_info(f"https://www.tiktok.com/@{username}", download=False)
            if 'entries' in info:
                for e in info['entries']:
                    if not e: continue
                    v_id = e.get('id')
                    url = e.get('url') or e.get('webpage_url') or f"https://www.tiktok.com/@{username}/video/{v_id}"
                    video_urls.append(url)
    except Exception as e:
        logger.error(f"Discovery Failed: {e}")
        return

    stats["total"] = len(video_urls)
    if stats["total"] == 0:
        logger.error("No videos found. Check cookies or profile.")
        return

    logger.info(f"Queue: {stats['total']} videos. Concurrent Workers: {CONCURRENT_VIDEOS}")

    with ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
        future_map = {executor.submit(download_worker, url, user_dir): url for url in video_urls}
        for future in as_completed(future_map):
            if shutdown_event.is_set():
                break
            result = future.result()
            with print_lock:
                stats["completed"] += 1
                remaining = stats["total"] - stats["completed"]
                # Clean UI output
                sys.stdout.write(f"\r[*] Progress: [{stats['completed']}/{stats['total']}] | Finished last: {future_map[future].split('/')[-1]}")
                sys.stdout.flush()

    print(f"\n[+] Done! @{username} cleared.")

if __name__ == "__main__":
    print("--- TikTok Turbo Downloader: BOSS MODE ---")
    u_input = input("Enter TikTok username: ")
    if u_input:
        fast_bulk_download(u_input)