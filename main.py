import yt_dlp
import os
import signal
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from yt_dlp.networking.impersonate import ImpersonateTarget

# --- CONFIGURATION ---
CONCURRENT_VIDEOS = 5 
CHROME_TARGET = ImpersonateTarget.from_str('chrome')
shutdown_event = Event()
print_lock = Lock() 

# Setup Professional Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("TikTokTurbo")

# Global Progress Counters
stats = {"total": 0, "completed": 0}

def get_ydl_opts(user_dir, is_extractor=False):
    """Factory for yt-dlp options with focus on speed and atomic integrity."""
    opts = {
        # FORCE best video and audio to merge into a single MP4
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'merge_output_format': 'mp4',
        'impersonate': CHROME_TARGET,
        'quiet': True,
        'no_warnings': True,
        'ffmpeg_location': 'C:/yt-dlp', # Ensure ffmpeg.exe is inside this folder
        'cookiefile': 'tiktok_cookies.txt',
        
        # 2026 Extraction Bypass
        'extractor_args': {
            'tiktok': {
                'web_id': 'random',
                'app_info': '1180',
            }
        },
        
        # Smart Resume logic
        'nopart': False, # Keep .part files so we can resume mid-download
    }
    
    if is_extractor:
        opts['extract_flat'] = True # Turbo-speed profile gathering
    else:
        opts.update({
            'outtmpl': f'{user_dir}/%(title).50s [%(id)s].%(ext)s',
            # This is the "Brain": Only records success to prevent half-assed files
            'download_archive': os.path.join(user_dir, 'archive.txt'),
            'concurrent_fragment_downloads': 5,
            'keepvideo': False, # Deletes the temporary audio/video parts after merging
        })
    return opts

def cleanup_partial_files(user_dir):
    """Deletes stray temporary files left over from a crash/forced shutdown."""
    if not os.path.exists(user_dir): return
    # Hit list for temporary fragments
    trash_exts = (".part", ".ytdl", ".mp3", ".m4a", ".f137", ".f251", ".f136")
    for file in os.listdir(user_dir):
        if file.endswith(trash_exts):
            try:
                os.remove(os.path.join(user_dir, file))
                logger.info(f"Cleaned up stray file: {file}")
            except:
                pass

def download_worker(url, user_dir):
    """The thread worker that handles one video download at a time."""
    if shutdown_event.is_set(): return
    
    try:
        # We create a new instance of YoutubeDL for each thread to ensure thread-safety
        with yt_dlp.YoutubeDL(get_ydl_opts(user_dir)) as ydl:
            ydl.download([url])
            
        with print_lock:
            stats["completed"] += 1
            remaining = stats["total"] - stats["completed"]
            logger.info(f"Progress: [{stats['completed']}/{stats['total']}] | Remaining: {remaining}")
            
    except Exception as e:
        if not shutdown_event.is_set():
            logger.error(f"Worker Error: {str(e)[:100]}")

def signal_handler(sig, frame):
    """Nuclear Exit: Kills the entire process instantly to fix the 'stuck' problem."""
    logger.warning("\n[!] INTERRUPT: Shutting down. Unfinished files stay as .part for resume.")
    shutdown_event.set()
    os._exit(1)

# Register the Ctrl+C signal
signal.signal(signal.SIGINT, signal_handler)

def fast_bulk_download(input_name):
    username = input_name.strip().replace("@", "")
    user_dir = os.path.join("downloads", username)
    
    # 1. Start-up Cleanup (Smart Resume)
    if not os.path.exists(user_dir):
        os.makedirs(user_dir)
    else:
        # Optional: cleanup_partial_files(user_dir) 
        # Only uncomment if you want to wipe .part files and start from 0% instead of resuming.
        pass

    # 2. DISCOVERY (Turbo Mode)
    logger.info(f"Gathering video list for @{username}...")
    video_urls = []
    try:
        with yt_dlp.YoutubeDL(get_ydl_opts(user_dir, is_extractor=True)) as ydl:
            info = ydl.extract_info(f"https://www.tiktok.com/@{username}", download=False)
            if 'entries' in info:
                for e in info['entries']:
                    if not e: continue
                    v_id = e.get('id')
                    # Robust URL builder to prevent 'webpage_url' errors
                    url = e.get('url') or e.get('webpage_url') or f"https://www.tiktok.com/@{username}/video/{v_id}"
                    video_urls.append(url)
    except Exception as e:
        logger.error(f"Discovery Failed: {e}")
        return

    stats["total"] = len(video_urls)
    if stats["total"] == 0:
        logger.error("No videos found. Check your cookies or connection.")
        return

    logger.info(f"Queue: {stats['total']} videos. Concurrent Workers: {CONCURRENT_VIDEOS}")

    # 3. CONCURRENT DOWNLOAD
    with ThreadPoolExecutor(max_workers=CONCURRENT_VIDEOS) as executor:
        futures = [executor.submit(download_worker, url, user_dir) for url in video_urls]
        try:
            while any(f.running() for f in futures):
                if shutdown_event.is_set(): break
                time.sleep(0.5)
        except KeyboardInterrupt:
            os._exit(1)

if __name__ == "__main__":
    print("--- TikTok Turbo Downloader (2026) ---")
    u_input = input("Enter TikTok username (e.g. @fanaikyyy): ")
    if u_input:
        fast_bulk_download(u_input)