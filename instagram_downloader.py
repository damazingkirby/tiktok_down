import instaloader
import os
import signal
import time
import glob
import threading
import queue
import re
import sys
import subprocess
from dataclasses import dataclass, field

# Rich UI Imports
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.table import Table
from rich.align import Align
from rich.text import Text

# --- GLOBALS & CONFIG ---
shutdown_event = threading.Event()
console = Console()
status_lock = threading.Lock()

def clean_msg(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', str(text)).strip()

def log_error(vid_or_msg, error_details=""):
    details = clean_msg(error_details)
    if not details: return
    with status_lock:
        with open('errors.log', 'a', encoding='utf-8') as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {vid_or_msg} - {details}\n")

class InstaEngine:
    """Instagram Scraper Engine (v1.1) with Session Support."""
    
    def __init__(self, username):
        self.username = username.strip().replace("@", "")
        self.user_dir = os.path.join("downloads", self.username)
        if not os.path.exists(self.user_dir):
            os.makedirs(self.user_dir)
            
        self.loader = instaloader.Instaloader(
            dirname_pattern=self.user_dir,
            filename_pattern='{date_utc}_UTC_{shortcode}',
            download_pictures=True,
            download_videos=True,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            post_metadata_txt_pattern=''
        )
        
        # Load session if it exists
        self.session_file = os.path.join("downloads", "insta_session")
        self.load_session()
        
        # Stats
        self.stats = {"total": 0, "completed": 0, "failed": 0, "start_time": time.time()}
        
        # Rich UI elements
        self.progress_bar = Progress(
            SpinnerColumn(), 
            TextColumn("[progress.description]{task.description}"),
            BarColumn(), 
            TaskProgressColumn(), 
            TimeElapsedColumn(),
        )
        self.main_task_id = self.progress_bar.add_task(f"[bold magenta]Scanning @{self.username}...[/]", total=None)

    def load_session(self):
        """Attempts to load a saved session to bypass 403/Login errors."""
        if os.path.exists(self.session_file):
            try:
                # We use a dummy username for loading if we don't know who saved the session
                self.loader.load_session_from_file("antigravity_user", filename=self.session_file)
            except Exception:
                pass

    def perform_login(self):
        """Interactive login to establish a session."""
        console.print(Panel("[bold yellow]Action Required[/]\nInstagram is blocking anonymous requests. You need to login once to continue.", style="yellow"))
        user = console.input("[bold blue]Instagram Username:[/] ")
        password = console.input("[bold blue]Instagram Password:[/] ", password=True)
        
        try:
            with console.status("[bold cyan]Authenticating...[/]"):
                self.loader.login(user, password)
                self.loader.save_session_to_file(filename=self.session_file)
            console.print("[bold green]✔ Login successful! Session saved.[/]")
            return True
        except Exception as e:
            console.print(f"[bold red]✖ Login failed:[/] {e}")
            return False

    def run_pipeline(self):
        """Scrapes the profile and downloads posts."""
        try:
            try:
                profile = instaloader.Profile.from_username(self.loader.context, self.username)
            except (instaloader.exceptions.LoginRequiredException, instaloader.exceptions.QueryReturnedBadRequestException, instaloader.exceptions.ProfileNotExistsException):
                # Often 403 or "not exists" is actually just a login wall
                if self.perform_login():
                    profile = instaloader.Profile.from_username(self.loader.context, self.username)
                else:
                    return

            # Update task total if possible
            total_posts = profile.mediacount
            self.progress_bar.update(self.main_task_id, total=total_posts, description=f"[bold cyan]Downloading @{self.username}[/]")
            self.stats["total"] = total_posts

            with Live(self.generate_ui(), refresh_per_second=4) as live:
                for post in profile.get_posts():
                    if shutdown_event.is_set():
                        break
                    
                    try:
                        # Skip existing files automatically by instaloader logic
                        self.loader.download_post(post, target=self.username)
                        
                        with status_lock:
                            self.stats["completed"] += 1
                        
                        self.progress_bar.update(self.main_task_id, advance=1)
                        live.update(self.generate_ui())
                        
                        # Anti-ban sleep
                        time.sleep(1.5)
                        
                    except Exception as e:
                        log_error(f"Post {post.shortcode} Failed", str(e))
                        with status_lock:
                            self.stats["failed"] += 1
                        self.progress_bar.update(self.main_task_id, advance=1)
                        live.update(self.generate_ui())

        except instaloader.exceptions.ProfileNotExistsException:
            console.print(f"[bold red]Error: Profile @{self.username} does not exist even after login.[/]")
        except instaloader.exceptions.PrivateProfileNotExistsException:
            console.print(f"[bold red]Error: @{self.username} is private. You must follow them to download.[/]")
        except Exception as e:
            import traceback
            log_error(f"Engine Error for @{self.username}", traceback.format_exc())
            console.print(f"[bold red]Critical Error:[/] {e}")
        finally:
            self.finish_report()

    def generate_ui(self):
        elapsed = time.time() - self.stats["start_time"]
        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
        
        summary = (
            f"[bold cyan]Total Posts:[/] {self.stats['total']}  |  [bold green]Success:[/] {self.stats['completed']}  |  "
            f"[bold red]Failed:[/] {self.stats['failed']}  |  [bold yellow]Uptime:[/] {elapsed_str}"
        )
        summary_panel = Panel(Align.center(summary), style="bold white on black", border_style="green")
        
        return Group(
            Panel(self.progress_bar, title="[bold magenta]Instagram Download Pipeline[/]", border_style="magenta"), 
            summary_panel
        )

    def finish_report(self):
        elapsed = time.time() - self.stats["start_time"]
        final_report = Group(
            Panel(f"Download Pipeline Completed in [bold cyan]{time.strftime('%H:%M:%S', time.gmtime(elapsed))}[/]", style="green"),
            Text(f"Target: {self.stats['total']} | Downloaded: {self.stats['completed']} | Failed: {self.stats['failed']}", style="bold white", justify="center")
        )
        console.print("\n")
        console.print(final_report)

def signal_handler(sig, frame):
    if shutdown_event.is_set(): os._exit(1)
    shutdown_event.set()
    console.print("\n[bold red blink]Halting... Cleaning up gracefully...[/]")

signal.signal(signal.SIGINT, signal_handler)

def check_dependencies():
    try:
        import instaloader
        import rich
    except ImportError:
        console.print("[bold yellow]Missing dependencies. Installing...[/]")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "instaloader", "rich"])
        console.print("[bold green]Dependencies installed successfully![/]")

if __name__ == "__main__":
    check_dependencies()
    import instaloader # Re-import after check
    
    console.print(Panel("[bold white]Instagram Profile Scraper (Instaloader Core)[/]", style="magenta"))
    u_input = console.input("[bold]Enter Instagram Username:[/] ")
    
    if u_input.strip():
        engine = InstaEngine(u_input)
        engine.run_pipeline()
    else:
        console.print("[bold red]No username entered. Exiting.[/]")
