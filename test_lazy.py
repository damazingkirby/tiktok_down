import yt_dlp
import time

def main():
    ydl_opts = {
        'extract_flat': True,
        'quiet': True,
        'extractor_args': {'tiktok': {'web_id': 'random', 'app_info': '1180', 'api_hostname': 'api16-normal-c-useast1a.tiktokv.com'}}
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        start = time.time()
        print("Extracting info...")
        info = ydl.extract_info('https://www.tiktok.com/@trop_belle1', download=False, process=False)
        print(f"Info returned in {time.time() - start:.2f} seconds")
        
        entries = info.get('entries')
        print(f"Type of entries: {type(entries)}")
        
        if hasattr(entries, '__iter__'):
            for i, e in enumerate(entries):
                if e is None:
                    continue # Some entries might be None if unparsed
                print(f"Found item {i+1}: {e.get('id')} at {time.time() - start:.2f} seconds")
                if i >= 4:
                    break
        else:
            print("No entries iterable")

if __name__ == '__main__':
    main()
