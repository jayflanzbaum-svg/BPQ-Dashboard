"""
N4SFL Dashboard Server — minimal local HTTP server.
Serves the dashboard HTML and provides an API endpoint for saving emails to the DB.
Runs on http://127.0.0.1:5999
"""

import json, sqlite3
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

SCRIPT_DIR = Path(__file__).parent
DB_FILE    = SCRIPT_DIR / "bpq_history.db"
HTML_FILE  = SCRIPT_DIR / "N4SFL_Dashboard.html"
PORT       = 5999

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
        server = HTTPServer(("127.0.0.1", PORT), Handler)
        print(f"N4SFL Dashboard Server  →  http://127.0.0.1:{PORT}")
        try: server.serve_forever()
        except KeyboardInterrupt: print("\nStopped.")
