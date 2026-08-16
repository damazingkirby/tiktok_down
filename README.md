# TikTok Video Downloader & Deep History Crawler

An automated, high-resilience TikTok video crawler and downloader featuring modern WAF anti-bot bypass mechanisms (`X-Bogus` & `X-Gnarly` signature generation), deep profile pagination, and dual-layer fallback video extraction engines.

---

## 📋 Prerequisites & Setup

Before running `manual_cookie.py`, ensure your environment and authentication cookies are prepared:

### 1. Python Environment (`uv`)
This project uses `uv` for fast dependency management. Ensure dependencies are installed:
```bash
uv pip install -r requirements.txt
```
*(If `TikTokDownloader-master` requirements are needed: `uv pip install -r TikTokDownloader-master/requirements.txt`)*

---

### 2. Export TikTok Cookies (`tiktok_cookie.txt`)
TikTok requires active session cookies to access video metadata and bypass challenge walls.

1. Open **TikTok** (`https://www.tiktok.com`) in your web browser and log in to your account.
2. Use a browser extension such as **Get cookies.txt LOCALLY** (Chrome/Firefox) or **Cookie-Editor**.
3. Export your cookies in **Netscape HTTP Cookie File** format.
4. Save the file in the project root directory as **`tiktok_cookie.txt`**:
   ```
   /home/ando/Documents/coding/tiktok_down/tiktok_cookie.txt
   ```
   > 💡 **Tip:** Keep this cookie file fresh. If TikTok invalidates the `ttwid` or session tokens (risk control / 风控), re-export the cookie file.

---

### 3. (Optional) Proxy List Setup (`proxies.txt`)
If you use rotating proxies to avoid IP rate limits:
Create `proxies.txt` in the root directory and add one proxy per line:
```text
http://user:pass@proxy1.example.com:8080
http://user:pass@proxy2.example.com:8080
```

---

## 🚀 How to Run `manual_cookie.py`

### Step 1: Execute Script
Launch the script using `uv`:
```bash
uv run python manual_cookie.py
```

### Step 2: Enter TikTok Username
When prompted in the interactive console:
```text
Enter Username: kessyyhr
```
*(You can type either `@kessyyhr` or `kessyyhr`)*

---

## ⚙️ How It Works (Pipeline Architecture)

1. **Cookie & Network Initialization**:
   - Loads active session cookies from `tiktok_cookie.txt`.
   - Initializes TLS impersonation (`chrome-133:macos-15`) to mirror real web browser behavior.

2. **Scout & Discovery Stage**:
   - Performs deep profile discovery across the target user's full post history.
   - Extracts unique Video IDs and original video titles.

3. **Execution & Download Stage**:
   - Downloads videos concurrently (`CONCURRENT_VIDEOS = 15`) with real-time UI tracking.
   - **Dual Engine Extraction**:
     - **Primary Engine**: Attempts direct video page extraction.
     - **Fallback Engine**: If TikTok blocks direct video access with a challenge wall (`Unexpected response from webpage request`), the crawler automatically routes through TikTok's embed fallback (`https://www.tiktok.com/embed/v2/<video_id>`).
   - Cleanly formats output files inside `downloads/<username>/`:
     ```
     downloads/kessyyhr/[7674683536573615367] GRWM for a concert come with me.mp4
     ```
   - Maintains an `archive.txt` file in the user directory to prevent downloading duplicates across multiple runs.

---

## 🛠️ Diagnostics & Logs

- **`errors.log`**: Logs any transient network warnings, challenge retries, or execution details.
- **`archive.txt`**: Recorded inside each target user's download directory (`downloads/<username>/archive.txt`) to skip previously downloaded videos.
