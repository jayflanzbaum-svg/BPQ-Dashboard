"""
N4SFL Dashboard Server — minimal local HTTP server.
Serves the dashboard HTML and provides an API endpoint for saving emails to the DB.
Runs on http://127.0.0.1:5999
"""

import json, os, sys, sqlite3, glob, subprocess, threading, time
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

SCRIPT_DIR = Path(__file__).parent
DB_FILE    = SCRIPT_DIR / "bpq_history.db"
HTML_FILE  = SCRIPT_DIR / "N4SFL_Dashboard.html"
PORT       = 5999

# ─── Log watching ─────────────────────────────────────────────────────────────
LOG_DIR          = r"C:\Users\Jason\AppData\Roaming\BPQ32\Logs"
LOG_PATTERNS     = ["log_*_BBS.txt", "CMSAccess_*.log", "ConnectLog_*.log", "log_*_DEBUG.txt"]
CHECK_INTERVAL   = 30   # seconds between checking for log changes
REFRESH_COOLDOWN = 60   # minimum seconds between dashboard rebuilds

_last_refresh_ts = time.time()


def _log_newest_mtime(log_dir):
    """Return the newest mtime across all BPQ32 log files."""
    newest = 0
    for pat in LOG_PATTERNS:
        for fp in glob.glob(os.path.join(log_dir, pat)):
            try:
                mt = os.path.getmtime(fp)
                if mt > newest:
                    newest = mt
            except OSError:
                pass
    return newest


def _watcher(log_dir):
    """Background thread: poll log directory and rebuild dashboard on changes."""
    global _last_refresh_ts
    last_mtime = _log_newest_mtime(log_dir)
    last_run   = time.time()
    while True:
        time.sleep(CHECK_INTERVAL)
        try:
            now_mtime = _log_newest_mtime(log_dir)
            now       = time.time()
            if now_mtime > last_mtime and (now - last_run) >= REFRESH_COOLDOWN:
                print(f"  Log changes detected — refreshing dashboard...")
                last_mtime = now_mtime
                last_run   = now
                try:
                    r = subprocess.run(
                        [sys.executable, str(SCRIPT_DIR / "bpq_dashboard.py")],
                        cwd=str(SCRIPT_DIR),
                        capture_output=True, text=True, timeout=300)
                    if r.returncode == 0:
                        _last_refresh_ts = time.time()
                        print(f"  Dashboard refreshed at {time.strftime('%H:%M:%S')}")
                    else:
                        print(f"  Refresh failed: {r.stderr[:200]}")
                except Exception as e:
                    print(f"  Refresh error: {e}")
            elif now_mtime > last_mtime:
                last_mtime = now_mtime
        except Exception as e:
            print(f"  Watcher error: {e}")

def db_get_emails():
    if not DB_FILE.exists(): return {}
    try:
        conn = sqlite3.connect(str(DB_FILE))
        conn.execute("CREATE TABLE IF NOT EXISTS emails(call TEXT PRIMARY KEY, email TEXT DEFAULT '')")
        rows = conn.execute("SELECT call, email FROM emails").fetchall()
        conn.close()
        return {r[0]: r[1] for r in rows if r[1]}
    except Exception: return {}

def db_save_email(call, email):
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute("CREATE TABLE IF NOT EXISTS emails(call TEXT PRIMARY KEY, email TEXT DEFAULT '')")
    conn.execute("INSERT INTO emails(call,email) VALUES(?,?) ON CONFLICT(call) DO UPDATE SET email=excluded.email",
                 (call.upper().strip(), email.lower().strip()))
    conn.commit(); conn.close()

def db_delete_email(call):
    if not DB_FILE.exists(): return
    conn = sqlite3.connect(str(DB_FILE))
    conn.execute("DELETE FROM emails WHERE call=?", (call.upper().strip(),))
    conn.commit(); conn.close()

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass

    def _send(self, code, body, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self): self._send(200, b"")

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/dashboard"):
            if HTML_FILE.exists():
                self._send(200, HTML_FILE.read_bytes(), "text/html; charset=utf-8")
            else:
                self._send(404, b'{"error":"Run refresh.bat first"}')
        elif path == "/api/emails":
            self._send(200, json.dumps(db_get_emails()).encode())
        elif path == "/api/last-refresh":
            self._send(200, json.dumps({"ts": _last_refresh_ts}).encode())
        elif path == "/api/status":
            self._send(200, json.dumps({"ok": True, "port": PORT}).encode())
        else:
            self._send(404, b'{"error":"not found"}')

    def do_POST(self):
        if urlparse(self.path).path == "/api/email":
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            try:
                data  = json.loads(body)
                call  = data.get("call","").upper().strip()
                email = data.get("email","").lower().strip()
                if not call:
                    self._send(400, b'{"error":"call required"}'); return
                if email: db_save_email(call, email)
                else:     db_delete_email(call)
                print(f"  {'Saved' if email else 'Removed'}: {call} → {email}")
                self._send(200, json.dumps({"ok": True, "call": call, "email": email}).encode())
            except Exception as e:
                self._send(500, json.dumps({"error": str(e)}).encode())
        else:
            self._send(404, b'{"error":"not found"}')

    def do_DELETE(self):
        path = urlparse(self.path).path
        if path.startswith("/api/email/"):
            db_delete_email(path.split("/")[-1])
            self._send(200, json.dumps({"ok": True}).encode())
        else:
            self._send(404, b'{"error":"not found"}')

def is_running():
    import socket
    try:
        with socket.create_connection(("127.0.0.1", PORT), timeout=0.5): return True
    except Exception: return False

if __name__ == "__main__":
    if is_running():
        print(f"Server already running on http://127.0.0.1:{PORT}")
    else:
        if os.path.isdir(LOG_DIR):
            t = threading.Thread(target=_watcher, args=(LOG_DIR,), daemon=True)
            t.start()
            print(f"  Log watcher active — checking every {CHECK_INTERVAL}s")
        else:
            print(f"  Log watcher skipped — directory not found: {LOG_DIR}")
        server = HTTPServer(("127.0.0.1", PORT), Handler)
        print(f"N4SFL Dashboard Server  →  http://127.0.0.1:{PORT}")
        try: server.serve_forever()
        except KeyboardInterrupt: print("\nStopped.")
