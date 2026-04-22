"""
N4SFL Dashboard Server — minimal local HTTP server.
Serves the dashboard HTML and provides an API endpoint for saving emails to the DB.
Runs on http://127.0.0.1:5999
"""

import json, os, sys, sqlite3, glob, subprocess, threading, time, configparser, smtplib
from pathlib import Path
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from urllib.request import urlopen
from email.mime.text import MIMEText

SCRIPT_DIR = Path(__file__).parent
DB_FILE    = SCRIPT_DIR / "bpq_history.db"
HTML_FILE  = SCRIPT_DIR / "N4SFL_Dashboard.html"
PORT       = 5999

# ─── Log watching ─────────────────────────────────────────────────────────────
LOG_DIR          = r"C:\Users\Jason\AppData\Roaming\BPQ32\Logs"
LOG_PATTERNS     = ["log_*_BBS.txt", "CMSAccess_*.log", "ConnectLog_*.log", "log_*_DEBUG.txt"]
CHECK_INTERVAL   = 30   # seconds between checking for log changes
REFRESH_COOLDOWN = 60   # minimum seconds between dashboard rebuilds

# ─── Liveness probe ───────────────────────────────────────────────────────────
PROBE_URL        = "http://127.0.0.1:8010/"
PROBE_TIMEOUT    = 2     # seconds — loopback, should be near-instant
PROBE_INTERVAL   = 60    # seconds between probes
NODE_STATE_FILE  = SCRIPT_DIR / "bpq_node_state.json"
REACHABILITY_LOG = SCRIPT_DIR / "bpq_reachability.log"
NOTIFY_LOG       = SCRIPT_DIR / "bpq_notifications.log"
CFG_FILE         = SCRIPT_DIR / "bpq_dashboard.cfg"

_last_refresh_ts = time.time()

# Shared node-state object. build_html reads it via the JSON file (separate
# process); handlers and probe thread access it directly via the lock.
_node_state = {
    "reachable":              None,    # None = not yet probed; True/False after
    "last_success":           None,    # ISO local datetime string
    "last_probe":             None,
    "consecutive_failures":   0,
    "downtime_start":         None,    # set when first failed probe of an outage
    "notified_down":          False,   # True after outage alert sent; reset on recovery
    "last_notify_attempt":    None,
    "last_notify_success":    None,
    "last_notify_error":      None,
    "recent_notify_failures": 0,
}
_node_state_lock = threading.Lock()
_notify_cfg      = {}     # populated at startup from [notifications] in cfg


def _ts_now_iso():
    """Local-time ISO timestamp string (seconds resolution)."""
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _ts_now_pretty():
    """Local 12-hour pretty time, e.g. '8:47 PM'."""
    return datetime.now().strftime("%I:%M %p").lstrip("0")


def _humanize_duration(seconds):
    """'8m 23s', '2 hr 13 min', '1 day 4 hr', etc."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    if h < 24:
        return f"{h} hr {m} min"
    d, h = divmod(h, 24)
    return f"{d} day {h} hr"


def _log_reachability(msg):
    try:
        with open(REACHABILITY_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
    except Exception:
        pass


def _log_notification(msg):
    try:
        with open(NOTIFY_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
    except Exception:
        pass


def _persist_node_state():
    """Write _node_state to disk so bpq_dashboard.py (separate process) can read it.
    Caller must hold _node_state_lock."""
    try:
        with open(NODE_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(_node_state, f, indent=2)
    except Exception as e:
        print(f"  Could not persist node state: {e}")


def _load_node_state_at_startup():
    """Restore last-known node state from disk if available — preserves
    notification gating and outage timing across server restarts."""
    if not NODE_STATE_FILE.exists():
        return
    try:
        with open(NODE_STATE_FILE, encoding="utf-8") as f:
            saved = json.load(f)
        with _node_state_lock:
            for k in _node_state:
                if k in saved:
                    _node_state[k] = saved[k]
        print(f"  Restored node state: reachable={_node_state['reachable']}, "
              f"notified_down={_node_state['notified_down']}")
    except Exception as e:
        print(f"  Could not restore node state: {e}")


def get_node_state():
    """Thread-safe snapshot of the current node state."""
    with _node_state_lock:
        return dict(_node_state)


def liveness_probe():
    """Single GET against the BPQ32 loopback web port. No auth, no parsing.
    Returns True if BPQ responds with any HTTP status within timeout, else False."""
    try:
        with urlopen(PROBE_URL, timeout=PROBE_TIMEOUT) as r:
            r.read(1)   # touch the socket; status code presence alone is enough
        return True
    except Exception:
        return False


# ─── Notification helpers ────────────────────────────────────────────────────
def _load_notify_cfg():
    """Load [notifications] section from bpq_dashboard.cfg. Sets _notify_cfg."""
    global _notify_cfg
    _notify_cfg = {"enabled": False}
    if not CFG_FILE.exists():
        return
    try:
        cfg = configparser.ConfigParser()
        cfg.read(CFG_FILE)
        if not cfg.has_section("notifications"):
            return
        s = cfg["notifications"]
        _notify_cfg = {
            "enabled":              s.getboolean("enabled", fallback=False),
            "smtp_host":            s.get("smtp_host", fallback=""),
            "smtp_port":            s.getint("smtp_port", fallback=465),
            "smtp_user":            s.get("smtp_user", fallback=""),
            "smtp_pass":            s.get("smtp_pass", fallback=""),
            "from_addr":            s.get("from_addr", fallback=""),
            "email_to":             s.get("email_to", fallback=""),
            "sms_to":               s.get("sms_to", fallback="").strip(),
            "alert_after_failures": s.getint("alert_after_failures", fallback=3),
        }
        if _notify_cfg["enabled"]:
            print(f"  Notifications enabled (alert after "
                  f"{_notify_cfg['alert_after_failures']} consecutive failed probes)")
    except Exception as e:
        print(f"  Could not load notification config: {e}")


def _send_email(to_addr, subject, body):
    """Send via SMTP_SSL. Returns (ok: bool, error_msg: str or '')."""
    if not to_addr:
        return (False, "no recipient configured")
    try:
        msg = MIMEText(body, _charset="utf-8")
        msg["Subject"] = subject
        msg["From"]    = _notify_cfg.get("from_addr", "")
        msg["To"]      = to_addr
        with smtplib.SMTP_SSL(_notify_cfg["smtp_host"],
                              _notify_cfg["smtp_port"], timeout=10) as smtp:
            smtp.login(_notify_cfg["smtp_user"], _notify_cfg["smtp_pass"])
            smtp.sendmail(_notify_cfg["from_addr"], [to_addr], msg.as_string())
        return (True, "")
    except Exception as e:
        return (False, f"{type(e).__name__}: {e}")


def _record_notify_attempt(ok, error_msg):
    """Update notify-health fields. Caller must hold _node_state_lock."""
    _node_state["last_notify_attempt"] = _ts_now_iso()
    if ok:
        _node_state["last_notify_success"]    = _ts_now_iso()
        _node_state["recent_notify_failures"] = 0
        _node_state["last_notify_error"]      = None
    else:
        _node_state["recent_notify_failures"] += 1
        _node_state["last_notify_error"]      = error_msg


def _fmt_iso_pretty(iso_str):
    """Convert an ISO local timestamp to '2026-04-21 8:47 PM' for display.
    Falls back to the raw string if parsing fails."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        return f"{dt.strftime('%Y-%m-%d')} {dt.strftime('%I:%M %p').lstrip('0')}"
    except Exception:
        return iso_str


def _send_outage_alert():
    """Send outage email + SMS. Updates notify-health fields. Logs every attempt.
    All user-facing timestamps in the body are formatted as local 12-hour AM/PM
    for consistency with the dashboard's other timestamp displays."""
    if not _notify_cfg.get("enabled"):
        return
    last_success_pretty = (_fmt_iso_pretty(_node_state.get("last_success"))
                           or "never (since dashboard started)")
    downtime_iso        = _node_state.get("downtime_start") or _ts_now_iso()
    downtime_pretty     = _fmt_iso_pretty(downtime_iso)
    consec_fails        = _node_state.get("consecutive_failures", 0)
    if _node_state.get("last_success"):
        try:
            ls = datetime.fromisoformat(_node_state["last_success"])
            elapsed = _humanize_duration((datetime.now() - ls).total_seconds())
        except Exception:
            elapsed = "?"
    else:
        elapsed = "n/a"

    subj  = "N4SFL-8 BBS unreachable"
    email_body = (
        f"BPQ32 is unreachable.\n"
        f"Last successful contact: {last_success_pretty} ({elapsed} ago).\n"
        f"Detected at: {downtime_pretty}.\n"
        f"Consecutive failed probes: {consec_fails}."
    )
    hhmm_now = datetime.now().strftime("%H:%M")
    if _node_state.get("last_success"):
        try:
            hhmm_last = datetime.fromisoformat(_node_state["last_success"]).strftime("%H:%M")
        except Exception:
            hhmm_last = "?"
    else:
        hhmm_last = "n/a"
    sms_body = f"N4SFL-8 DOWN {hhmm_now} last ok {hhmm_last}"

    for to_addr, body, kind in [
        (_notify_cfg.get("email_to"), email_body, "email"),
        (_notify_cfg.get("sms_to"),   sms_body,   "sms"),
    ]:
        if not to_addr:
            continue
        ok, err = _send_email(to_addr, subj, body)
        _record_notify_attempt(ok, err)
        if ok:
            _log_notification(f"outage alert sent ({kind}) to {to_addr}")
        else:
            _log_notification(f"outage alert FAILED ({kind}) to {to_addr}: {err}")


def _send_recovery_alert():
    """Send recovery email + SMS with outage duration. Caller has updated
    last_success but downtime_start is still populated. Timestamps formatted
    as local 12-hour AM/PM for consistency with the rest of the UI."""
    if not _notify_cfg.get("enabled"):
        return
    downtime_iso  = _node_state.get("downtime_start") or _ts_now_iso()
    recovery_iso  = _ts_now_iso()
    downtime_pretty = _fmt_iso_pretty(downtime_iso)
    recovery_pretty = _fmt_iso_pretty(recovery_iso)
    try:
        d_start = datetime.fromisoformat(downtime_iso)
        duration_s = (datetime.now() - d_start).total_seconds()
        duration   = _humanize_duration(duration_s)
    except Exception:
        duration_s = 0
        duration   = "?"

    subj = "N4SFL-8 BBS recovered"
    email_body = (
        f"BPQ32 is reachable again.\n"
        f"Down from {downtime_pretty} to {recovery_pretty}.\n"
        f"Outage duration: {duration}."
    )
    hhmm_now = datetime.now().strftime("%H:%M")
    sms_body = f"N4SFL-8 UP {hhmm_now} down {duration}"

    for to_addr, body, kind in [
        (_notify_cfg.get("email_to"), email_body, "email"),
        (_notify_cfg.get("sms_to"),   sms_body,   "sms"),
    ]:
        if not to_addr:
            continue
        ok, err = _send_email(to_addr, subj, body)
        _record_notify_attempt(ok, err)
        if ok:
            _log_notification(f"recovery alert sent ({kind}) to {to_addr}")
        else:
            _log_notification(f"recovery alert FAILED ({kind}) to {to_addr}: {err}")


def _apply_probe_result(reachable):
    """Single source of truth for state transitions. Caller must NOT hold the
    lock; this function takes it. Returns the previous reachable value so
    callers can detect transitions."""
    fired_outage = False
    fired_recovery = False
    duration_s = 0
    with _node_state_lock:
        prev = _node_state["reachable"]
        _node_state["last_probe"] = _ts_now_iso()
        if reachable:
            _node_state["consecutive_failures"] = 0
            _node_state["last_success"]         = _ts_now_iso()
            was_down = (prev is False) or _node_state.get("notified_down")
            downtime_start = _node_state.get("downtime_start")
            _node_state["downtime_start"] = None
            _node_state["reachable"]      = True
            if prev is False:
                # transition unreachable -> reachable
                if downtime_start:
                    try:
                        d_start = datetime.fromisoformat(downtime_start)
                        duration_s = (datetime.now() - d_start).total_seconds()
                    except Exception:
                        duration_s = 0
                    _log_reachability(
                        f"unreachable -> reachable (recovered after "
                        f"{_humanize_duration(duration_s)})"
                    )
                else:
                    _log_reachability("unreachable -> reachable")
            elif prev is None:
                _log_reachability("startup probe: reachable")
            if was_down and _node_state.get("notified_down"):
                fired_recovery = True
        else:
            _node_state["consecutive_failures"] += 1
            if _node_state["downtime_start"] is None:
                _node_state["downtime_start"] = _ts_now_iso()
            _node_state["reachable"] = False
            if prev is True:
                _log_reachability("reachable -> unreachable")
            elif prev is None:
                _log_reachability("startup probe: unreachable")
            threshold = _notify_cfg.get("alert_after_failures", 3)
            if (_node_state["consecutive_failures"] >= threshold
                    and not _node_state["notified_down"]):
                fired_outage = True
        _persist_node_state()

    # Send notifications outside the lock (SMTP can be slow).
    if fired_outage:
        _send_outage_alert()
        with _node_state_lock:
            _node_state["notified_down"] = True
            _persist_node_state()
    if fired_recovery:
        _send_recovery_alert()
        with _node_state_lock:
            _node_state["notified_down"] = False
            _persist_node_state()


def _probe_thread():
    """Daemon: probe BPQ32 every PROBE_INTERVAL seconds. Fires immediately
    on startup, then on a fixed cadence."""
    while True:
        try:
            _apply_probe_result(liveness_probe())
        except Exception as e:
            print(f"  Probe thread error: {e}")
        time.sleep(PROBE_INTERVAL)


def force_probe():
    """Trigger an immediate liveness probe (used by manual rebuild / refresh
    handlers so manual actions always reflect current reality)."""
    try:
        _apply_probe_result(liveness_probe())
    except Exception as e:
        print(f"  Force-probe error: {e}")


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


def _trigger_rebuild():
    """Run bpq_dashboard.py once and update the last-refresh timestamp on success.
    Used by the manual /api/refresh-lists endpoint to skip the watcher poll wait."""
    global _last_refresh_ts
    try:
        r = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "bpq_dashboard.py")],
            cwd=str(SCRIPT_DIR),
            capture_output=True, text=True, timeout=300)
        if r.returncode == 0:
            _last_refresh_ts = time.time()
            print(f"  Manual rebuild done at {time.strftime('%H:%M:%S')}")
        else:
            print(f"  Manual rebuild failed: {r.stderr[:500]}")
    except Exception as e:
        print(f"  Manual rebuild error: {e}")


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
                        print(f"  Refresh failed: {r.stderr[:500]}")
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
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
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
        path = urlparse(self.path).path
        if path == "/api/email":
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
        elif path == "/api/refresh-lists":
            # Invalidate the cached partner/user lists and trigger a dashboard rebuild.
            # Next bpq_dashboard.py run will do a live fetch (no cache file → live fetch).
            # Force-probe first so manual actions reflect current reality.
            cache_file = SCRIPT_DIR / "bpq_lists_cache.json"
            try:
                force_probe()
                if cache_file.exists():
                    cache_file.unlink()
                print(f"  Manual list refresh requested — cache invalidated, rebuilding...")
                # Trigger a rebuild in the same process (don't wait for the watcher poll)
                threading.Thread(target=_trigger_rebuild, daemon=True).start()
                self._send(200, json.dumps({"ok": True, "msg": "lists invalidated; rebuilding"}).encode())
            except Exception as e:
                self._send(500, json.dumps({"error": str(e)}).encode())
        elif path == "/api/rebuild":
            # Synchronous rebuild — runs bpq_dashboard.py and returns when complete.
            # Wired to the split button's "Refresh" action so the user always
            # sees a freshly-generated dashboard, not whatever the watcher last
            # produced. Force-probe first so reachable state is current.
            print(f"  Manual rebuild requested...")
            try:
                force_probe()
                _trigger_rebuild()
                self._send(200, json.dumps({"ok": True, "msg": "rebuild complete"}).encode())
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
        # Notification config + last-known node state restored before any thread starts.
        _load_notify_cfg()
        _load_node_state_at_startup()

        if os.path.isdir(LOG_DIR):
            t = threading.Thread(target=_watcher, args=(LOG_DIR,), daemon=True)
            t.start()
            print(f"  Log watcher active — checking every {CHECK_INTERVAL}s")
        else:
            print(f"  Log watcher skipped — directory not found: {LOG_DIR}")

        # Liveness probe: fires immediately, then every PROBE_INTERVAL seconds.
        threading.Thread(target=_probe_thread, daemon=True).start()
        print(f"  Liveness probe active — {PROBE_URL} every {PROBE_INTERVAL}s")

        server = HTTPServer(("127.0.0.1", PORT), Handler)
        print(f"N4SFL Dashboard Server  →  http://127.0.0.1:{PORT}")
        try: server.serve_forever()
        except KeyboardInterrupt: print("\nStopped.")
