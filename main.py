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
COOKIE_FILE = 'tiktok_cookies.txt'
CHROME_TARGET = ImpersonateTarget.from_str('chrome')
shutdown_event = Event()
print_lock = Lock()

# Custom Logger for the script
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("TikTokTurbo")

# Silent Logger to kill yt-dlp internal spam
class QuietLogger:
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass

stats = {"total": 0, "completed": 0, "failed": 0, "skipped": 0}

def get_ydl_opts(user_dir, is_extractor=False):
    opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'merge_output_format': 'mp4',
        'impersonate': CHROME_TARGET,
        'quiet': True,
        'no_warnings': True,
        'logger': QuietLogger(),  # This silences the ERROR: [TikTok] spam
        'ffmpeg_location': 'C:/yt-dlp',
        'cookiefile': COOKIE_FILE,
        'extractor_args': {'tiktok': {'web_id': 'random', 'app_info': '1180'}},
        'socket_timeout': 20,
        'retries': 5,
        'connector_args': {'force_no_http2': True}, 
    }
    
    if is_extractor:
        opts['extract_flat'] = True
    else:
        opts.update({
            'outtmpl': f'{user_dir}/%(title).50s [%(id)s].%(ext)s',
            'download_archive': os.path.join(user_dir, 'archive.txt'),
            'concurrent_fragment_downloads': 5,
        })
    return opts

def download_worker(url, user_dir):
    if shutdown_event.is_set(): return (False, "Shutdown")
    
    attempts = 0
    while attempts < 2:
        try:
            with yt_dlp.YoutubeDL(get_ydl_opts(user_dir)) as ydl:
                ydl.download([url])
            return True, None
        except Exception as e:
            attempts += 1
            err = str(e).lower()
            if "no video formats" in err:
                time.sleep(5) # Brief wait for TikTok to cool down
                continue
            if "cookie" in err and "netscape" in err:
                return False, "INVALID COOKIE FORMAT"
            return False, "BLOCKED/PRIVATE"
            
    return False, "MAX RETRIES (No Formats Found)"

def fast_bulk_download(input_name):
    os.system('') # ANSI Support
    
    # 1. Surgical Cookie Check
    if not os.path.exists(COOKIE_FILE):
        logger.error(f"Missing {COOKIE_FILE}!")
        return
    
    with open(COOKIE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        if "# Netscape" not in f.readline():
            logger.error(f"FATAL: {COOKIE_FILE} is not in Netscape format. Export as 'Netscape' from your browser extension.")
            return

    username = input_name.strip().replace("@", "")
    user_dir = os.path.join("downloads", username)
    if not os.path.exists(user_dir): os.makedirs(user_dir)

    logger.info(f"Gathering video list for @{username}...")
    video_urls = []
    
    try:
        with yt_dlp.YoutubeDL(get_ydl_opts(user_dir, is_extractor=True)) as ydl:
            info = ydl.extract_info(f"https://www.tiktok.com/@{username}", download=False)
            if 'entries' in info:
                video_urls = [f"https://www.tiktok.com/@{username}/video/{e['id']}" for e in info['entries'] if e]
    except Exception as e:
        logger.error(f"Discovery Failed: {e}")
        return

    stats["total"] = len(video_urls)
    if stats["total"] == 0:
        logger.error("No videos found.")
        return

    logger.info(f"Queue: {stats['total']} videos | Workers: {CONCURRENT_VIDEOS}")

    with ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
        future_map = {executor.submit(download_worker, url, user_dir): url for url in video_urls}
        
        for future in as_completed(future_map):
            if shutdown_event.is_set(): break
            
            success, error = future.result()
            url_id = future_map[future].split('/')[-1]
            
            with print_lock:
                if success:
                    stats["completed"] += 1
                else:
                    stats["failed"] += 1
                    # Log errors only to the log file or as clean warnings
                    sys.stdout.write(f"\n[!] Issue with {url_id}: {error}\n")
                
                # Dynamic Clean Progress Bar
                processed = stats["completed"] + stats["failed"]
                sys.stdout.write(f"\r[*] Progress: [{processed}/{stats['total']}] | OK: {stats['completed']} | ERR: {stats['failed']}")
                sys.stdout.flush()

    print(f"\n\n[+] Done! @{username} cleared.")
    print(f"Final Count -> Success: {stats['completed']} | Problems: {stats['failed']}")

def signal_handler(sig, frame):
    shutdown_event.set()
    os._exit(1)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    print("--- TikTok Turbo Downloader: BOSS MODE v2.1 ---")
    u_input = input("Enter TikTok username: ")
    if u_input:
        fast_bulk_download(u_input)