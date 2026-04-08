#!/usr/bin/env python3
"""
N4SFL BBS Log Parser — generates a standalone HTML dashboard
Reads BPQ32 logs from: C:/Users/Jason/AppData/Roaming/BPQ32/Logs

Usage:
    python bpq_dashboard.py
    python bpq_dashboard.py --days 7
    python bpq_dashboard.py --out dashboard.html
    python bpq_dashboard.py --qrz-user N8FLA --qrz-pass yourpassword

QRZ credentials can also be stored in bpq_dashboard.cfg (same folder):
    [qrz]
    username = N8FLA
    password = yourpassword

Requirements: Python 3.8+, no pip installs needed.
"""

import os, re, sys, glob, json, time, argparse, configparser
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError


# ─── CONFIG ────────────────────────────────────────────────────────────────────

LOG_DIR   = r"C:\Users\Jason\AppData\Roaming\BPQ32\Logs"
OUT_FILE  = "N4SFL_Dashboard.html"
DAYS_BACK = 0   # 0 = all available log files; set to e.g. 30 to limit

HOME_CALL = "N4SFL"
HOME_GRID = "EL96XL"
HOME_LAT  = 26.46
HOME_LNG  = -80.10
OP_CALL   = "N8FLA"
LOCATION  = "Delray Beach, FL"

# Outlook web URL — compose a new email. Change to outlook.live.com if personal account.
OUTLOOK_URL = "https://outlook.office.com/mail/deeplink/compose"

CACHE_FILE    = "qrz_cache.json"
QRZ_URL       = "https://xmldata.qrz.com/xml/current/"
QRZ_THROTTLE  = 0.5   # seconds between requests


# ─── MAIDENHEAD → LAT/LNG ─────────────────────────────────────────────────────

def grid_to_latlon(grid: str):
    """Convert a Maidenhead locator (4 or 6 chars) to (lat, lng) centre point."""
    g = grid.upper().strip()
    if len(g) < 4:
        return None
    try:
        lng = (ord(g[0]) - ord('A')) * 20 - 180
        lat = (ord(g[1]) - ord('A')) * 10 - 90
        lng += int(g[2]) * 2
        lat += int(g[3]) * 1
        if len(g) >= 6:
            lng += (ord(g[4]) - ord('A')) * (2/24) + (1/24)
            lat += (ord(g[5]) - ord('A')) * (1/24) + (0.5/24)
        else:
            lng += 1.0
            lat += 0.5
        return round(lat, 4), round(lng, 4)
    except (ValueError, IndexError):
        return None


# ─── QRZ CLIENT ────────────────────────────────────────────────────────────────

class QRZClient:
    """Minimal QRZ XML API client with local disk caching."""

    def __init__(self, username: str, password: str, cache_file: str = CACHE_FILE):
        self.username   = username
        self.password   = password
        self.cache_file = cache_file
        self.session    = None
        self.cache      = self._load_cache()
        self._last_req  = 0.0

    def _load_cache(self) -> dict:
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, encoding="utf-8") as f:
                    data = json.load(f)
                print(f"  QRZ cache: {len(data)} entries loaded from {self.cache_file}")
                return data
            except Exception:
                pass
        return {}

    def _save_cache(self):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            print(f"  Warning: could not save cache: {e}")

    def _get(self, params: dict):
        elapsed = time.time() - self._last_req
        if elapsed < QRZ_THROTTLE:
            time.sleep(QRZ_THROTTLE - elapsed)
        url = QRZ_URL + "?" + urlencode(params)
        try:
            req = Request(url, headers={"User-Agent": "N4SFL-BPQ-Dashboard/1.0"})
            with urlopen(req, timeout=4) as resp:
                self._last_req = time.time()
                return ET.fromstring(resp.read())
        except (URLError, HTTPError) as e:
            print(f"  QRZ HTTP error: {e}")
        except ET.ParseError as e:
            print(f"  QRZ XML parse error: {e}")
        return None

    def _ns(self, root) -> str:
        m = re.match(r"\{[^}]+\}", root.tag)
        return m.group(0) if m else ""

    def _reachable(self) -> bool:
        """Single quick check — is QRZ reachable? Avoids N×timeout if offline."""
        try:
            urlopen(QRZ_URL, timeout=4)
            return True
        except Exception:
            return False

    def login(self) -> bool:
        print(f"  Logging in to QRZ as {self.username}...")
        root = self._get({"username": self.username, "password": self.password,
                          "agent": "N4SFL-BPQ-Dashboard/1.0"})
        if root is None:
            return False
        ns  = self._ns(root)
        key = root.find(f"{ns}Session/{ns}Key")
        err = root.find(f"{ns}Session/{ns}Error")
        if err is not None:
            print(f"  QRZ login error: {err.text}")
            return False
        if key is not None and key.text:
            self.session = key.text
            print(f"  QRZ session ok.")
            return True
        return False

    def lookup(self, callsign: str, _retry: bool = False):
        call = callsign.upper().strip()

        # Cache hit — but re-fetch if email field is missing (stale pre-email cache entry)
        if call in self.cache:
            cached = self.cache[call]
            if cached is not None and "email" not in cached:
                pass  # fall through to re-fetch
            else:
                return cached

        # Need session
        if not self.session:
            if not self.login():
                return None

        root = self._get({"s": self.session, "callsign": call})
        if root is None:
            return None

        ns  = self._ns(root)
        err = root.find(f"{ns}Session/{ns}Error")
        if err is not None:
            txt = (err.text or "").lower()
            # Expired session — retry once only (guard against infinite recursion)
            if ("session" in txt or "invalid" in txt) and not _retry:
                self.session = None
                if not self.login():
                    return None
                return self.lookup(call, _retry=True)
            # Not found or second failure — cache as None so we don't re-query
            self.cache[call] = None
            self._save_cache()
            return None

        rec = root.find(f"{ns}Callsign")
        if rec is None:
            return None

        def txt(tag):
            el = rec.find(f"{ns}{tag}")
            return el.text.strip() if el is not None and el.text else ""

        def esc(s):
            """HTML-escape a string for safe injection into popup HTML."""
            return (s.replace("&", "&amp;")
                     .replace("<", "&lt;")
                     .replace(">", "&gt;")
                     .replace('"', "&quot;"))

        lat_s = txt("lat")
        lng_s = txt("lon")

        result = {
            "call":    call,
            "lat":     float(lat_s) if lat_s else None,
            "lng":     float(lng_s) if lng_s else None,
            "grid":    esc(txt("grid")),
            "name":    esc(f"{txt('fname')} {txt('name')}".strip()),
            "city":    esc(txt("addr2")),
            "state":   esc(txt("state")),
            "country": esc(txt("country")),
            "email":   txt("email"),   # pulled from QRZ XML
        }

        # Derive coords from grid if QRZ didn't return them
        if (result["lat"] is None or result["lng"] is None) and result["grid"]:
            coords = grid_to_latlon(result["grid"])
            if coords:
                result["lat"], result["lng"] = coords

        parts = [p for p in [result["city"], result["state"], result["country"]] if p]
        result["location"] = ", ".join(parts)

        print(f"  QRZ {call}: {result['location'] or '?'} "
              f"({result['lat']}, {result['lng']}) grid={result['grid']}")

        self.cache[call] = result
        self._save_cache()
        return result


# ─── LOG PARSING ───────────────────────────────────────────────────────────────

def strip_ssid(call: str) -> str:
    return call.split("-")[0].upper()

def find_logs(log_dir: str, days: int) -> dict:
    files = {"bbs": [], "cms": [], "connect": [], "debug": []}
    patterns = [("bbs","log_*_BBS.txt"),("cms","CMSAccess_*.log"),
                ("connect","ConnectLog_*.log"),("debug","log_*_DEBUG.txt")]
    if days == 0:
        # Grab every matching file regardless of age
        for kind, pat in patterns:
            files[kind] = sorted(glob.glob(os.path.join(log_dir, pat)))
    else:
        cutoff = datetime.now() - timedelta(days=days)
        for kind, pat in patterns:
            for fp in sorted(glob.glob(os.path.join(log_dir, pat))):
                try:
                    if datetime.fromtimestamp(os.path.getmtime(fp)) >= cutoff:
                        files[kind].append(fp)
                except OSError:
                    pass
    return files

def read_file(path: str) -> list:
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            with open(path, encoding=enc, errors="replace") as f:
                return f.readlines()
        except Exception:
            continue
    return []


class Stats:
    def __init__(self):
        self.cms_polls     = 0
        self.inbound_total = 0
        self.crashes       = 0
        self.daily = defaultdict(lambda: {
            "cms":0,"inbound":0,"msgs":0,
            "unique":set(),      # all callsigns seen that day
            "bbs":set(),         # BBS callers that day
            "gw":set()           # gateway users that day
        })
        self.alerts        = []
        self.infos         = []
        self.date_range    = (None, None)
        self.bbs_callers        = {}   # call → {connects, modes, grid} — ALL inbound
        self.inbound_b2_calls   = set()# calls that used B2 forwarding protocol inbound
        self.inbound_b2_msgs    = {}   # call → {received, sent} message counts inbound
        self.gateway_users      = {}   # base_call → {sessions, bytes_sent, bytes_rcvd, grid, client, dates}
        self.forward_peers      = {}   # peer → {attempts, successes} — all time
        self.forward_peers_daily = {}  # peer → {iso_date → {attempts, successes}}
        self.grids         = {}   # base_call → grid from logs
        self.station_dates = {}   # base_call → set of ISO date strings active
        self.crash_dates   = []   # ISO dates of crashes
        self.new_bbs_guests = set() # callsigns appearing for first time this run

    def record_grid(self, call: str, grid: str):
        base = strip_ssid(call)
        g = (grid or "").strip().upper()
        if g and len(g) >= 4 and base not in self.grids:
            self.grids[base] = g

    def record_active(self, call: str, file_date_6: str):
        """Record that a station was active on a given YYMMDD date."""
        base = strip_ssid(call)
        if base.startswith("N4SFL"):
            return
        try:
            iso = datetime.strptime("20" + file_date_6, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return
        if base not in self.station_dates:
            self.station_dates[base] = set()
        self.station_dates[base].add(iso)


GRID_RE = re.compile(r"\(([A-R]{2}\d{2}(?:[A-X]{2})?)\)", re.IGNORECASE)


def fmt_time_12h(t: str) -> str:
    """Convert HH:MM:SS 24h to h:MM AM/PM local."""
    try:
        h, m, s = t.split(":")
        h = int(h)
        period = "AM" if h < 12 else "PM"
        h12 = h % 12 or 12
        return f"{h12}:{m} {period}"
    except Exception:
        return t


def parse_debug(files, s: Stats):
    # Line format in DEBUG log: YYMMDD HH:MM:SS ! Program Starting
    time_re = re.compile(r"^(\d{6})\s+(\d{2}:\d{2}:\d{2})")
    first_start_seen = False
    for fp in sorted(files):   # chronological order
        m = re.search(r"(\d{6})", os.path.basename(fp))
        file_date = m.group(1) if m else "?"
        for line in read_file(fp):
            if "Program Starting" in line:
                s.crashes += 1
                tm = time_re.match(line)
                time_str = tm.group(2) if tm else "00:00:00"
                try:
                    iso = datetime.strptime("20" + file_date, "%Y%m%d").strftime("%Y-%m-%d")
                    dt_label = f"{iso} at {fmt_time_12h(time_str)} (local)"
                    if not first_start_seen:
                        first_start_seen = True
                        s.crash_dates.append({"iso": iso, "dt": dt_label, "startup": True})
                    else:
                        s.crash_dates.append({"iso": iso, "dt": dt_label, "startup": False})
                except ValueError:
                    pass


def parse_cms_access(files, s: Stats):
    conn_re   = re.compile(r"^(\d{2}:\d{2}:\d{2})\s+\d+\s+(\S+)\s+Connected to CMS")
    disc_re   = re.compile(r"Disconnected.*Bytes Sent\s*=\s*(\d+)\s+Bytes Received\s+(\d+)")
    client_re = re.compile(r"\[(RMS Express|BPQ|RadioMail|Winlink)[^\]]*\]")
    fc_re     = re.compile(r"^\S+\s+\d+\s+FC EM ")
    # ;SR: <bytes_rcvd> <freq_hz> <snr>  — freq 0=local, 1.8M-30M=HF, 30M+=FM/VHF
    sr_re     = re.compile(r"^[\d:]+\s+\d+\s+;SR:\s+\d+\s+(\d+)")
    all_dates = []

    def freq_to_mode(freq_hz: int) -> str:
        if freq_hz == 0:
            return ""
        if 1_800_000 <= freq_hz <= 30_000_000:
            return "VARA HF"
        if freq_hz > 30_000_000:
            return "VARA FM"
        return "VARA HF"  # default — almost always HF for Winlink

    for fp in files:
        m = re.search(r"(\d{8})", os.path.basename(fp))
        file_date = m.group(1)[2:] if m else "?"
        all_dates.append(file_date)
        current_call   = None
        session_active = False

        for line in read_file(fp):
            line = line.rstrip()

            cm = conn_re.match(line)
            if cm:
                current_call   = cm.group(2)
                session_active = True
                if current_call.startswith("N4SFL"):
                    s.cms_polls += 1
                    s.daily[file_date]["cms"] += 1
                else:
                    base = strip_ssid(current_call)
                    if base not in s.gateway_users:
                        s.gateway_users[base] = {
                            "sessions":0,"bytes_sent":0,"bytes_rcvd":0,
                            "grid":"","client":"","dates":[],"modes":set(),"msgs":0
                        }
                    s.gateway_users[base]["sessions"] += 1
                    if file_date not in s.gateway_users[base]["dates"]:
                        s.gateway_users[base]["dates"].append(file_date)
                    s.record_active(base, file_date)
                    s.daily[file_date]["gw"].add(base)
                    s.daily[file_date]["unique"].add(base)
                continue

            if not session_active:
                continue

            # Frequency from ;SR: line → VARA HF or VARA FM
            srm = sr_re.match(line)
            if srm and current_call and not current_call.startswith("N4SFL"):
                freq_hz = int(srm.group(1))
                mode    = freq_to_mode(freq_hz)
                if mode:
                    base = strip_ssid(current_call)
                    if base in s.gateway_users:
                        s.gateway_users[base]["modes"].add(mode)

            gm = GRID_RE.search(line)
            if gm and current_call:
                s.record_grid(current_call, gm.group(1))
                base = strip_ssid(current_call)
                if base in s.gateway_users and not s.gateway_users[base]["grid"]:
                    s.gateway_users[base]["grid"] = gm.group(1).upper()

            clm = client_re.search(line)
            if clm and current_call and not current_call.startswith("N4SFL"):
                base = strip_ssid(current_call)
                if base in s.gateway_users:
                    s.gateway_users[base]["client"] = clm.group(0).strip("[]")

            if fc_re.match(line):
                s.daily[file_date]["msgs"] += 1
                if current_call and not current_call.startswith("N4SFL"):
                    base = strip_ssid(current_call)
                    if base in s.gateway_users:
                        s.gateway_users[base]["msgs"] += 1

            dm = disc_re.search(line)
            if dm:
                sent = int(dm.group(1)); rcvd = int(dm.group(2))
                if current_call and not current_call.startswith("N4SFL"):
                    base = strip_ssid(current_call)
                    if base in s.gateway_users:
                        s.gateway_users[base]["bytes_sent"] += sent
                        s.gateway_users[base]["bytes_rcvd"] += rcvd
                session_active = False; current_call = None

    if all_dates:
        s.date_range = (sorted(all_dates)[0], sorted(all_dates)[-1])


def parse_connect_log(files, s: Stats):
    re_conn = re.compile(
        r"^(\d{2}:\d{2}:\d{2})\s+Call from\s+(\S+)(?:\s+at Node \S+)?\s+to\s+(\S+)\s+Mode\s+(.+)"
    )
    # Gateway ports — connects here are Winlink RMS users
    GATEWAY_PORTS = {"N4SFL-10", "N4SFL-11"}

    for fp in files:
        m = re.search(r"(\d{6})", os.path.basename(fp))
        file_date = m.group(1) if m else "?"
        for line in read_file(fp):
            cm = re_conn.match(line.strip())
            if cm:
                call = strip_ssid(cm.group(2))
                dest = cm.group(3)
                mode = cm.group(4).strip()   # e.g. "VARA HF", "VARA FM", "AX.25"
                # Skip own auto-connects
                if call in ("N4SFL",):
                    continue
                # Connects to gateway port = Winlink RMS users, not BBS callers.
                # Capture their mode then skip BBS accounting.
                if dest in GATEWAY_PORTS:
                    base = strip_ssid(call)
                    if base in s.gateway_users:
                        # Track all modes this station has used
                        s.gateway_users[base].setdefault("modes", set()).add(mode)
                    s.record_active(call, file_date)
                    continue
                s.inbound_total += 1
                s.daily[file_date]["inbound"] += 1
                s.daily[file_date]["unique"].add(call)
                s.daily[file_date]["bbs"].add(call)
                if call not in s.bbs_callers:
                    s.bbs_callers[call] = {"connects":0,"modes":set(),"grid":""}
                s.bbs_callers[call]["connects"] += 1
                s.bbs_callers[call]["modes"].add(mode)
                s.record_active(call, file_date)


def parse_bbs_log(files, s: Stats):
    conn_out_re = re.compile(r"^(\d{6})\s+\S+\s+\|(\w[\w-]*)\s+Connecting to BBS (\w[\w-]*)")
    radio_re    = re.compile(r"^(\d{6})\s+\S+\s+>(\w[\w-]*)\s+RADIO\s+([\d.]+)")
    ok_re       = re.compile(r"\*\*\* Connected to|}\s+Connected to")
    fail_re     = re.compile(r"VARA\} Failure|Failure with|Can't Connect|Error - Port|Channel is busy")
    call_in_re  = re.compile(r"\|(\w[\w-]*)\s+Incoming Connect from (\w[\w-]+)")

    def freq_to_mode(freq_mhz: float) -> str:
        if 1.8 <= freq_mhz <= 30.0:
            return "VARA HF"
        if freq_mhz > 30.0:
            return "VARA FM"
        return "VARA HF"  # default — almost always HF for BBS forwarding

    for fp in files:
        current_peer    = None
        peer_attempt    = False
        last_incoming   = None
        inbound_b2      = False   # current inbound session is using B2 protocol
        inbound_msgs_rx = 0       # messages received this inbound session
        inbound_msgs_tx = 0       # messages sent this inbound session
        current_freq    = None    # frequency for current outbound attempt
        line_date6      = None    # YYMMDD from current line

        for line in read_file(fp):
            # Extract date from start of line (format: YYMMDD HH:MM:SS ...)
            dm = re.match(r"^(\d{6})\s", line)
            if dm:
                line_date6 = dm.group(1)
            try:
                line_iso = datetime.strptime("20" + line_date6, "%Y%m%d").strftime("%Y-%m-%d") if line_date6 else ""
            except ValueError:
                line_iso = ""

            # Outbound connect attempt
            co = conn_out_re.match(line)
            if co:
                current_peer  = strip_ssid(co.group(3))
                peer_attempt  = True
                current_freq  = None  # reset — RADIO line comes after
                if current_peer in ("RMS", "N4SFL", HOME_CALL):
                    peer_attempt = False
                    continue
                if current_peer not in s.forward_peers:
                    s.forward_peers[current_peer] = {"attempts":0,"successes":0,"modes":set()}
                s.forward_peers[current_peer]["attempts"] += 1
                if current_peer not in s.forward_peers_daily:
                    s.forward_peers_daily[current_peer] = {}
                if line_iso not in s.forward_peers_daily.get(current_peer, {}):
                    s.forward_peers_daily[current_peer][line_iso] = {"attempts":0,"successes":0}
                s.forward_peers_daily[current_peer][line_iso]["attempts"] += 1
                continue

            # RADIO line — frequency for current outbound attempt
            rm = radio_re.match(line)
            if rm and peer_attempt and current_peer:
                try:
                    freq_mhz = float(rm.group(3))
                    current_freq = freq_mhz
                    mode = freq_to_mode(freq_mhz)
                    s.forward_peers[current_peer]["modes"].add(mode)
                    # Store mode in daily entry too
                    if current_peer in s.forward_peers_daily and line_iso in s.forward_peers_daily[current_peer]:
                        entry = s.forward_peers_daily[current_peer][line_iso]
                        if "modes" not in entry:
                            entry["modes"] = []
                        if mode not in entry["modes"]:
                            entry["modes"].append(mode)
                except ValueError:
                    pass
                continue

            if peer_attempt and current_peer:
                if ok_re.search(line):
                    s.forward_peers[current_peer]["successes"] += 1
                    if current_peer in s.forward_peers_daily and line_iso in s.forward_peers_daily.get(current_peer, {}):
                        s.forward_peers_daily[current_peer][line_iso]["successes"] += 1
                    # If no frequency was set for this attempt, detect NETROM from the } pattern
                    if current_freq is None and "} Connected to" in line:
                        s.forward_peers[current_peer]["modes"].add("NETROM")
                    peer_attempt = False
                elif fail_re.search(line):
                    peer_attempt = False

            ic = call_in_re.search(line)
            if ic:
                # Close out previous inbound session tracking
                if last_incoming and inbound_b2:
                    base = strip_ssid(last_incoming)
                    s.inbound_b2_calls.add(base)
                    if base not in s.inbound_b2_msgs:
                        s.inbound_b2_msgs[base] = {"received": 0, "sent": 0}
                    s.inbound_b2_msgs[base]["received"] += inbound_msgs_rx
                    s.inbound_b2_msgs[base]["sent"]     += inbound_msgs_tx
                last_incoming   = strip_ssid(ic.group(2))
                inbound_b2      = False
                inbound_msgs_rx = 0
                inbound_msgs_tx = 0

            # B2 forwarding protocol detection on inbound sessions
            if last_incoming and not last_incoming.startswith(HOME_CALL):
                # FA = file-announce (they're offering a message)
                if re.search(r"<\S+\s+FA [BPT] ", line):
                    inbound_b2 = True
                    inbound_msgs_rx += 1
                # FA outbound = we're offering them a message
                elif re.search(r">\S+\s+FA [BPT] ", line):
                    inbound_b2 = True
                    inbound_msgs_tx += 1
                # B2 handshake or FQ = session was definitely forwarding
                elif re.search(r"[<>]\S+\s+\[BPQ-", line) or re.search(r"[<>]\S+\s+\[WL2K-", line):
                    inbound_b2 = True

            # Inbound disconnect — save B2 status
            if last_incoming and re.search(r"\|" + re.escape(last_incoming) + r"\s+" + re.escape(last_incoming) + r"\s+Disconnected", line):
                if inbound_b2:
                    base = strip_ssid(last_incoming)
                    s.inbound_b2_calls.add(base)
                    if base not in s.inbound_b2_msgs:
                        s.inbound_b2_msgs[base] = {"received": 0, "sent": 0}
                    s.inbound_b2_msgs[base]["received"] += inbound_msgs_rx
                    s.inbound_b2_msgs[base]["sent"]     += inbound_msgs_tx
                last_incoming   = None
                inbound_b2      = False
                inbound_msgs_rx = 0
                inbound_msgs_tx = 0

            gm = GRID_RE.search(line)
            if gm and last_incoming:
                s.record_grid(last_incoming, gm.group(1))
                if last_incoming in s.bbs_callers and not s.bbs_callers[last_incoming]["grid"]:
                    s.bbs_callers[last_incoming]["grid"] = gm.group(1).upper()


# ─── GEO RESOLUTION ────────────────────────────────────────────────────────────

def resolve_geo(calls: list, s: Stats, qrz) -> dict:
    results = {}
    total   = len(calls)
    # Single reachability check — if QRZ is offline, skip all lookups immediately
    if qrz and not qrz._reachable():
        print("  QRZ unreachable — using grid data only.")
        qrz = None

    for i, call in enumerate(calls, 1):
        base = strip_ssid(call)
        if base in results:
            continue
        print(f"  [{i}/{total}] {base}...", end=" ", flush=True)

        # 1. QRZ
        if qrz:
            info = qrz.lookup(base)
            if info and info.get("lat") is not None:
                results[base] = {**info, "source": "qrz"}
                continue

        # 2. Grid from logs
        grid = (s.grids.get(base) or
                s.bbs_callers.get(base, {}).get("grid","") or
                s.gateway_users.get(base, {}).get("grid",""))
        if grid:
            coords = grid_to_latlon(grid)
            if coords:
                print(f"grid {grid}")
                results[base] = {"lat":coords[0],"lng":coords[1],"grid":grid,
                                 "name":"","location":f"grid {grid}","source":"grid"}
                continue

        print("no location")
        results[base] = None

    return results


# ─── HTML BUILD ────────────────────────────────────────────────────────────────

def email_cell(email: str, call: str) -> str:
    """Return a table cell with envelope icon (if email known) and edit pencil."""
    import urllib.parse
    pencil = ("<svg xmlns='http://www.w3.org/2000/svg' width='13' height='13' viewBox='0 0 24 24' "
              "fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>"
              "<path d='M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7'/>"
              "<path d='M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z'/></svg>")
    env = ("<svg xmlns='http://www.w3.org/2000/svg' width='15' height='15' viewBox='0 0 24 24' "
           "fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'>"
           "<rect x='2' y='4' width='20' height='16' rx='2'/><polyline points='22,4 12,13 2,4'/></svg>")
    edit_btn = (f"<button onclick=\"editEmail('{call}','{email}')\" "
                f"title='Edit email' "
                f"style='background:none;border:none;cursor:pointer;color:#64748b;padding:2px;margin-left:4px;"
                f"vertical-align:middle;opacity:.6' onmouseover=\"this.style.opacity=1\" onmouseout=\"this.style.opacity=.6\">"
                f"{pencil}</button>")
    if email:
        subject = urllib.parse.quote(f"Ham radio — {call} via N4SFL-8")
        href    = f"{OUTLOOK_URL}?to={urllib.parse.quote(email)}&subject={subject}"
        return (f"<td style='text-align:center;white-space:nowrap'>"
                f"<a href='{href}' target='_blank' title='{email}' "
                f"style='color:#3b82f6;vertical-align:middle;text-decoration:none'>{env}</a>"
                f"{edit_btn}</td>")
    else:
        return (f"<td style='text-align:center'>"
                f"<span style='color:#cbd5e1;vertical-align:middle'>—</span>"
                f"{edit_btn}</td>")


def pct(a, b):
    return int(100 * a / b) if b else 0

def mode_tags(modes: set) -> str:
    mp = {
        "VARA HF": "tv",
        "VARA FM": "tv2",
        "NETROM":  "tn",
        "AX.25":   "ta",
    }
    # Upgrade any bare VARA to VARA HF
    normalized = {"VARA HF" if m == "VARA" else m for m in modes}
    parts = []
    for m in sorted(normalized):
        cls = mp.get(m, "tw")
        parts.append(f"<span class='tag {cls}'>{m}</span>")
    return " ".join(parts)

def fmt_date(d6: str) -> str:
    """Convert 6-digit YYMMDD to 'Mar 31' style label."""
    try:
        return datetime.strptime("20" + d6, "%Y%m%d").strftime("%b %d")
    except ValueError:
        return d6

def fmt_bytes(n: int) -> str:
    if n == 0: return "—"
    if n < 1024: return f"{n} B"
    if n < 1_048_576: return f"{n/1024:.1f} KB"
    return f"{n/1_048_576:.1f} MB"

def haversine_mi(lat1: float, lng1: float, lat2: float, lng2: float) -> int:
    """Great-circle distance in miles between two lat/lng points."""
    from math import radians, sin, cos, sqrt, atan2
    R = 3958.8
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng/2)**2
    return int(R * 2 * atan2(sqrt(a), sqrt(1-a)))

def _station_type(base: str, s: Stats) -> str:
    """Return map marker type for a station based on how they interacted."""
    is_guest   = base in {strip_ssid(c) for c in s.bbs_callers} and base not in s.inbound_b2_calls
    is_partner = base in s.inbound_b2_calls
    is_gw      = base in {strip_ssid(c) for c in s.gateway_users}
    roles = sum([is_guest, is_partner, is_gw])
    if roles > 1:    return "multi"
    if is_partner:   return "partner"
    if is_guest:     return "guest"
    return "gw"


def build_html(s: Stats, geo: dict, days: int, email_overrides: dict = None) -> str:
    if email_overrides is None:
        email_overrides = {}
    # Merge email overrides into geo — override takes precedence over QRZ
    for call, em in email_overrides.items():
        if call in geo and geo[call]:
            geo[call]["email"] = em
        else:
            geo[call] = geo.get(call) or {}
            geo[call]["email"] = em
    now   = datetime.now().strftime("%Y-%m-%d %H:%M")
    dr    = s.date_range
    dlabel = (f"{fmt_date(dr[0])} \u2013 {fmt_date(dr[1])}, 20{dr[1][:2]}"
              if dr != (None, None) else "All available data")
    n_bbs_callers  = len(s.bbs_callers)   # overwritten below after interactive/forwarder split
    n_gw_users     = len(s.gateway_users)
    unique         = len(set(list(s.bbs_callers) + list(s.gateway_users)))
    n_crashes      = max(0, s.crashes - 1)
    crash_col      = "#f85149" if n_crashes else "#1a9e5c"

    # daily rows — with data-date for filtering and data-v for sorting
    daily_rows = ""
    for d, v in sorted(s.daily.items()):
        try:
            iso = datetime.strptime("20" + d, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            iso = d
        cms_c    = v['cms']
        inb_c    = v['inbound']
        msgs_c   = v['msgs']
        uniq_c   = len(v['unique'])
        daily_rows += (
            f"<tr data-date='{iso}'>"
            f"<td data-v='{iso}'>{fmt_date(d)}, {iso[:4]}</td>"
            f"<td data-v='{cms_c}' style='text-align:center'>{cms_c}</td>"
            f"<td data-v='{inb_c}' style='text-align:center'>{inb_c}</td>"
            f"<td data-v='{msgs_c}' style='text-align:center'>{msgs_c}</td>"
            f"<td data-v='{uniq_c}' style='text-align:center'>{uniq_c}</td>"
            f"</tr>"
        )

    # mode bars
    mc: dict = {}
    for cv in s.bbs_callers.values():
        for m in cv["modes"]:
            mc[m] = mc.get(m, 0) + cv["connects"]
    max_mc = max(mc.values()) if mc else 1
    mcol = {"VARA HF":"#1a9e5c","VARA HF":"#1a9e5c","NETROM":"#1f6feb","AX.25":"#e3b341"}
    mode_bars_parts = []
    for m, cnt in sorted(mc.items(), key=lambda x: x[1], reverse=True):
        col = mcol.get(m, "#8957e5")
        mode_bars_parts.append(
            f"<div class='br'><div class='bl'>{m}</div>"
            f"<div class='bt'><div class='bf' style='width:{pct(cnt,max_mc)}%;background:{col}'></div></div>"
            f"<div class='bc'>{cnt}</div></div>"
        )
    mode_bars = "".join(mode_bars_parts)

    # Split bbs_callers into: interactive users (humans) vs inbound forwarders (peer BBSs)
    interactive_callers = {k:v for k,v in s.bbs_callers.items()
                           if strip_ssid(k) not in s.inbound_b2_calls}
    inbound_forwarders  = {k:v for k,v in s.bbs_callers.items()
                           if strip_ssid(k) in s.inbound_b2_calls}

    # Interactive user bars
    top_interactive = sorted(interactive_callers.items(), key=lambda x: x[1]["connects"], reverse=True)[:9]
    max_int = top_interactive[0][1]["connects"] if top_interactive else 1
    caller_rows = ""
    for call, cv in top_interactive:
        base  = strip_ssid(call)
        dot_c = "#8b5cf6" if base in s.gateway_users else "#3b82f6"
        w     = pct(cv["connects"], max_int)
        dates_str = ",".join(sorted(s.station_dates.get(base, set())))
        caller_rows += (
            f"<div class='br guest-bar' data-dates='{dates_str}'>"
            f"<div style='width:8px;height:8px;border-radius:50%;background:{dot_c};flex-shrink:0'></div>"
            f"<div class='bl'>{call}</div>"
            f"<div class='bt'><div class='bf' style='width:{w}%;background:#3b82f6'></div></div>"
            f"<div class='bc'>{cv['connects']}</div>"
            f"<div style='white-space:nowrap'>{mode_tags(cv['modes'])}</div></div>"
        )
    if not caller_rows:
        caller_rows = "<div style='color:#94a3b8;font-size:.8rem;padding:8px 0'>No Guest BBS users in this period.</div>"

    # Inbound forwarder rows
    top_fwd = sorted(inbound_forwarders.items(), key=lambda x: x[1]["connects"], reverse=True)[:9]
    max_fwd = top_fwd[0][1]["connects"] if top_fwd else 1
    inbound_fwd_rows = ""
    for call, cv in top_fwd:
        base  = strip_ssid(call)
        msgs  = s.inbound_b2_msgs.get(base, {})
        rx    = msgs.get("received", 0)
        tx    = msgs.get("sent", 0)
        w     = pct(cv["connects"], max_fwd)
        msg_str   = f"+{rx} rcvd / {tx} sent" if (rx or tx) else ""
        dates_str = ",".join(sorted(s.station_dates.get(base, set())))
        inbound_fwd_rows += (
            f"<div class='br partner-bar' data-dates='{dates_str}'>"
            f"<div style='width:8px;height:8px;border-radius:50%;background:#f97316;flex-shrink:0'></div>"
            f"<div class='bl'>{call}</div>"
            f"<div class='bt'><div class='bf' style='width:{w}%;background:#f97316'></div></div>"
            f"<div class='bc'>{cv['connects']}</div>"
            f"<div style='white-space:nowrap'>{mode_tags(cv['modes'])}</div>"
            f"{f'<div style=\"font-size:.72em;color:#94a3b8;margin-left:4px\">{msg_str}</div>' if msg_str else ''}"
            f"</div>"
        )
    if not inbound_fwd_rows:
        inbound_fwd_rows = "<div style='color:#94a3b8;font-size:.8rem;padding:8px 0'>No inbound BBS forwarding partners in this period.</div>"

    # Updated unique counts
    n_bbs_callers      = len(interactive_callers)
    n_inbound_forwarders = len(inbound_forwarders)

    # peer rows — with mode tags derived from RADIO frequency lines
    _peer_mode_cls = {"VARA HF":"tv","VARA FM":"tv2","VARA HF":"tv","AX.25":"ta","NETROM":"tn"}
    peer_rows = ""
    for peer, pv in sorted(s.forward_peers.items(), key=lambda x: x[1]["attempts"], reverse=True)[:10]:
        pp    = pct(pv["successes"], pv["attempts"])
        bc    = "#22c55e" if pp >= 70 else ("#f59e0b" if pp >= 20 else "#ef4444")
        lc    = "#22c55e" if pp >= 70 else ("#f59e0b" if pp >= 20 else "#ef4444")
        gp    = geo.get(peer)
        loc   = ""
        if gp:
            raw = gp.get("location","") or gp.get("grid","")
            if raw: loc = f" <span style='color:#94a3b8;font-size:.75em'>({raw})</span>"
        modes = pv.get("modes", set())
        mode_tag_parts = [
            f"<span class='tag {_peer_mode_cls.get(m,'tw')}'>{m}</span>"
            for m in sorted(modes)
        ]
        mode_tags_html = " ".join(mode_tag_parts)
        fail_c = pv["attempts"] - pv["successes"]
        peer_rows += (
            f"<div style='margin:10px 0'>"
            f"<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;flex-wrap:wrap;gap:4px'>"
            f"<span style='font-size:.85em;font-weight:600'>{peer}{loc}</span>"
            f"<span style='display:flex;align-items:center;gap:6px'>"
            f"{mode_tags_html}"
            f"<span style='font-size:.8em;color:{lc}'>{pv['successes']} ok / {fail_c} freq-attempts</span>"
            f"</span></div>"
            f"<div class='bt' style='height:8px'><div class='bf' style='width:{max(2,pp)}%;background:{bc}'></div></div>"
            f"</div>"
        )

    # BBS users table rows (all bbs_callers: guest + partner)
    bbs_table_rows = ""
    for call, cv in sorted(s.bbs_callers.items(), key=lambda x: x[1]["connects"], reverse=True):
        base       = strip_ssid(call)
        is_partner = base in s.inbound_b2_calls
        role_val   = "partner" if is_partner else "guest"
        role_tag   = ("<span class='tag' style='background:rgba(249,115,22,.12);color:#f97316;border:1px solid rgba(249,115,22,.3)'>Partner</span>"
                      if is_partner else
                      "<span class='tag' style='background:rgba(59,130,246,.12);color:#3b82f6;border:1px solid rgba(59,130,246,.3)'>Guest</span>")
        gc         = geo.get(base)
        loc        = (gc.get("location","") or cv.get("grid","") or "—") if gc else (cv.get("grid","") or "—")
        dist_h     = "—"
        dist_sort  = 99999
        if gc and gc.get("lat") is not None:
            try:
                mi = haversine_mi(HOME_LAT, HOME_LNG, gc["lat"], gc["lng"])
                dist_h    = f"{mi} mi"
                dist_sort = mi
            except Exception:
                pass
        modes_h      = mode_tags(cv.get("modes", set()))
        mode_sort_val = " ".join(sorted(cv.get("modes", set())))
        msgs_d       = s.inbound_b2_msgs.get(base, {})
        msgs_str     = f"{msgs_d.get('received',0)} rcvd / {msgs_d.get('sent',0)} sent" if is_partner and msgs_d else "—"
        connects_val = cv["connects"] if cv["connects"] > 0 else "—"
        connects_sort = str(cv["connects"])
        last_con     = cv.get("last_connect", "")
        # Use actual station_dates — populated from logs AND from bbs_users.txt last_connect dates
        # If station_dates is empty for this user → dates_active=[] → JS shows them only under All Data
        all_dates    = sorted(s.station_dates.get(base, set()))
        dates_active = all_dates
        last_active  = all_dates[-1] if all_dates else (parse_bpq_date(last_con) if last_con else "")
        last_active_h = last_active if last_active else (f"<span style='color:#94a3b8;font-size:.8em'>{last_con}</span>" if last_con else "—")
        email        = (gc.get("email","") if gc else "")
        bbs_table_rows += (
            f"<tr data-call='{call}' data-dates='{','.join(dates_active)}'>"
            f"<td style='font-weight:600' data-v='{call}'>{call}</td>"
            f"<td data-v='{role_val}'>{role_tag}</td>"
            f"<td style='color:#8b949e;font-size:.8em' data-v='{loc.lower()}'>{loc}</td>"
            f"<td style='color:#8b949e;font-size:.8em' data-v='{dist_sort}'>{dist_h}</td>"
            f"<td data-v='{mode_sort_val}'>{modes_h}</td>"
            f"<td style='text-align:center' data-v='{last_active}'>{last_active_h}</td>"
            f"<td style='text-align:center' data-v='{connects_sort}'>{connects_val}</td>"
            f"<td style='font-size:.8em' data-v='{msgs_str}'>{msgs_str}</td>"
            f"{email_cell(email, call)}"
            f"<td style='color:#22c55e'>&#10003;</td></tr>"
        )

    # gateway rows — with distance
    _gw_mode_cls = {"VARA HF":"tv","VARA FM":"tv2","AX.25":"ta","NETROM":"tn"}
    gw_rows = ""
    for call, gv in sorted(s.gateway_users.items(), key=lambda x: x[1]["sessions"], reverse=True):
        grid  = gv.get("grid","—") or "—"
        cli   = gv.get("client","") or ""
        cli_h = f"<span class='tag tw'>{cli.replace('RMS Express','RMS Exp')}</span>" if cli else "—"
        modes = gv.get("modes", set())
        # If we have specific VARA HF or VARA FM, drop the generic "VARA" fallback
        specific = {m for m in modes if m in ("VARA HF", "VARA FM")}
        display_modes = specific if specific else modes
        mode_parts = []
        for m in sorted(display_modes):
            cls = _gw_mode_cls.get(m, "tw")
            mode_parts.append(f"<span class='tag {cls}'>{m}</span>")
        mode_h = " ".join(mode_parts) if mode_parts else "—"
        gc    = geo.get(call)
        loc   = (gc.get("location","") or grid) if gc else grid
        # Distance for sorting (numeric miles, or 99999 if unknown)
        dist_sort = 99999
        dist_h = "—"
        if gc and gc.get("lat") is not None:
            try:
                mi = haversine_mi(HOME_LAT, HOME_LNG, gc["lat"], gc["lng"])
                dist_h = f"{mi} mi"
                dist_sort = mi
            except Exception:
                pass
        # Dates active
        dates_active = sorted(s.station_dates.get(call, set()))
        sessions    = gv["sessions"]
        loc_sort    = loc.lower() if loc != "—" else "zzz"
        mode_sort = " ".join(sorted(modes)) if modes else "zzz"
        cli_sort  = cli.lower() if cli else "zzz"
        total_bytes = gv.get("bytes_sent",0) + gv.get("bytes_rcvd",0)
        msgs_count  = gv.get("msgs", 0)
        data_str    = fmt_bytes(total_bytes)
        gw_email  = gc.get("email","") if gc else ""
        gw_rows += (
            f"<tr data-call='{call}' data-dates='{','.join(dates_active)}'>"
            f"<td style='font-weight:600' data-v='{call}'>{call}</td>"
            f"<td style='color:#8b949e;font-size:.8em' data-v='{loc_sort}'>{loc}</td>"
            f"<td style='color:#8b949e;font-size:.8em' data-v='{dist_sort}'>{dist_h}</td>"
            f"<td data-v='{mode_sort}'>{mode_h}</td>"
            f"<td data-v='{cli_sort}'>{cli_h}</td>"
            f"<td style='text-align:center' data-v='{sessions}'>{sessions}</td>"
            f"<td style='text-align:center' data-v='{msgs_count}'>{msgs_count if msgs_count else '—'}</td>"
            f"<td style='text-align:right;font-size:.8em' data-v='{total_bytes}'>{data_str}</td>"
            f"{email_cell(gw_email, call)}"
            f"<td style='color:#22c55e'>&#10003;</td></tr>"
        )

    # events as JS-filterable objects
    events_list = []
    # New guest BBS users — first time seen
    today_iso = datetime.now().strftime("%Y-%m-%d")
    for call in sorted(s.new_bbs_guests):
        # Get their most recent known date for context
        dates = sorted(s.station_dates.get(call, set()))
        last = dates[-1] if dates else today_iso
        cv   = s.bbs_callers.get(call, {})
        last_con = cv.get("last_connect","")
        body = f"First time seen on your BBS."
        if last_con:
            body += f" Last connect: {last_con}."
        events_list.append({
            "type":  "info",
            "date":  last,
            "title": f"New guest BBS user: {call}",
            "body":  body
        })
    # Crashes — skip the very first (normal startup)
    for cd in s.crash_dates:
        if cd.get("startup"):
            continue
        events_list.append({
            "type": "crash",
            "date": cd["iso"],
            "title": f"Crash / restart — {cd['dt']}",
            "body": "BPQ32 terminated unexpectedly and auto-restarted. Check for mail loss around this time. MiniDump file present in log directory."
        })
    # Forwarding failures
    for peer, pv in s.forward_peers.items():
        if pv["attempts"] >= 10 and pv["successes"] == 0:
            events_list.append({
                "type": "warn",
                "date": "",   # no single date — covers full period
                "title": f"{peer}: complete forwarding failure",
                "body": f"{pv['attempts']} attempts, 0 successes \u2014 station may be off-air."
            })
    # Good news
    good = [(p,v) for p,v in s.forward_peers.items() if pct(v["successes"],v["attempts"]) >= 80]
    if good:
        best = sorted(good, key=lambda x: x[1]["successes"], reverse=True)[0]
        events_list.append({
            "type": "ok",
            "date": "",
            "title": f"{best[0]}: best forwarding partner",
            "body": f"{best[1]['successes']} successful forwards, {pct(best[1]['successes'],best[1]['attempts'])}% success rate."
        })
    for a in s.alerts:
        events_list.append({"type":"warn","date":"","title":a,"body":""})
    for i in s.infos:
        events_list.append({"type":"ok","date":"","title":i,"body":""})
    if not events_list:
        events_list.append({"type":"ok","date":"","title":"No significant issues detected","body":""})

    import json as _json
    events_json      = _json.dumps(events_list)
    peer_daily_json  = _json.dumps(s.forward_peers_daily)

    # map station data
    entries = [
        f"  {{call:{repr(HOME_CALL+'-8 / '+OP_CALL)},lat:{HOME_LAT},lng:{HOME_LNG},"
        f"type:'home',grid:{repr(HOME_GRID)},dates:[],"
        f"info:'Home station &mdash; {LOCATION}<br>BPQ32 BBS + Winlink Gateway<br>{OP_CALL}'}}"
    ]
    for call in sorted(set(list(s.bbs_callers) + list(s.gateway_users))):
        base = strip_ssid(call)
        if base.startswith(HOME_CALL):
            continue
        g = geo.get(base)
        if not g or g.get("lat") is None:
            continue
        stype  = _station_type(base, s)
        bbs_c  = s.bbs_callers.get(base, {}).get("connects", 0)
        gw_s   = s.gateway_users.get(base, {}).get("sessions", 0)
        name   = g.get("name", "")
        loc    = g.get("location", "") or g.get("grid", "")
        grid   = g.get("grid", "") or s.grids.get(base, "")
        src    = g.get("source", "")
        dates  = sorted(s.station_dates.get(base, set()))
        parts  = []
        if name:  parts.append(name)
        if loc:   parts.append(loc)
        if bbs_c: parts.append(f"BBS connects: {bbs_c}")
        if gw_s:  parts.append(f"GW sessions: {gw_s}")
        if src:   parts.append(f"via {src}")
        entries.append(
            f"  {{call:{repr(base)},lat:{round(g['lat'],4)},lng:{round(g['lng'],4)},"
            f"type:{repr(stype)},grid:{repr(grid)},info:{repr('<br>'.join(parts))},"
            f"dates:{repr(dates)}}}"
        )
    stations_js = "[\n" + ",\n".join(entries) + "\n]"

    # Per-day data embedded as JSON so the client-side date filter can recompute KPIs
    daily_json_rows = []
    for d, v in sorted(s.daily.items()):
        # Build ISO date string from YYMMDD
        try:
            iso = datetime.strptime("20" + d, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            iso = d
        daily_json_rows.append(
            f"  {{date:{repr(iso)},label:{repr(fmt_date(d))},"
            f"cms:{v['cms']},inbound:{v['inbound']},msgs:{v['msgs']},"
            f"bbs:{len(v['bbs'])},gw:{len(v['gw'])},unique:{len(v['unique'])}}}"
        )
    daily_json = "[\n" + ",\n".join(daily_json_rows) + "\n]"

    days_label = "all logs" if days == 0 else f"{days}-day window"
    crash_col  = "#f85149" if n_crashes else "#22c55e"
    crash_dates_json = _json.dumps([cd["iso"] for cd in s.crash_dates if not cd.get("startup")])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{HOME_CALL}-8 BBS Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css"/>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
html{{font-size:15px}}
body{{background:#f8fafc;color:#1e293b;font-family:'Inter',sans-serif;line-height:1.5;min-height:100vh}}
body.dark{{background:#0f172a;color:#e2e8f0}}

/* ── layout ── */
.wrap{{max-width:1280px;margin:0 auto;padding:24px 20px}}

/* ── header ── */
.hdr{{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:24px;flex-wrap:wrap}}
.hdr-icon{{width:44px;height:44px;background:rgba(20,184,166,.1);border:1px solid rgba(20,184,166,.25);border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:22px;flex-shrink:0}}
.hdr-left{{display:flex;align-items:flex-start;gap:12px}}
.hdr-title{{font-size:1.35rem;font-weight:700;color:#0f172a;letter-spacing:-.3px;line-height:1.2}}
.hdr-title .accent{{color:#0d9488}}
body.dark .hdr-title{{color:#f1f5f9}}
body.dark .hdr-title .accent{{color:#2dd4bf}}
.hdr-sub{{font-size:.8rem;color:#64748b;margin-top:3px}}
.hdr-sub .mono{{font-family:'JetBrains Mono',monospace;font-size:.78rem;color:#0d9488;font-weight:600}}
body.dark .hdr-sub{{color:#94a3b8}}
body.dark .hdr-sub .mono{{color:#2dd4bf}}
.tog{{cursor:pointer;background:#fff;border:1px solid #e2e8f0;border-radius:20px;padding:6px 14px;font-size:.78rem;color:#475569;font-family:inherit;font-weight:500;display:flex;align-items:center;gap:6px;box-shadow:0 1px 2px rgba(0,0,0,.05)}}
body.dark .tog{{background:#1e293b;border-color:#334155;color:#94a3b8}}

/* ── KPI cards ── */
.kpi-row{{display:grid;grid-template-columns:repeat(5,1fr);gap:14px;margin-bottom:24px}}
.kpi{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
body.dark .kpi{{background:#1e293b;border-color:#334155}}
.kpi-blue  {{background:rgba(59,130,246,.08) !important;border-color:rgba(59,130,246,.3) !important}}
.kpi-purple{{background:rgba(139,92,246,.08) !important;border-color:rgba(139,92,246,.3) !important}}
.kpi-orange{{background:rgba(249,115,22,.08) !important;border-color:rgba(249,115,22,.3) !important}}
.kpi-teal  {{background:rgba(13,148,136,.08) !important;border-color:rgba(13,148,136,.3) !important}}
.kpi-red   {{background:rgba(239,68,68,.08)  !important;border-color:rgba(239,68,68,.3)  !important}}
.kpi-green {{background:rgba(34,197,94,.08)  !important;border-color:rgba(34,197,94,.3)  !important}}
body.dark .kpi-blue  {{background:rgba(59,130,246,.1) !important;border-color:rgba(59,130,246,.3) !important}}
body.dark .kpi-purple{{background:rgba(139,92,246,.1) !important;border-color:rgba(139,92,246,.3) !important}}
body.dark .kpi-orange{{background:rgba(249,115,22,.1) !important;border-color:rgba(249,115,22,.3) !important}}
body.dark .kpi-teal  {{background:rgba(13,148,136,.1) !important;border-color:rgba(13,148,136,.3) !important}}
body.dark .kpi-red   {{background:rgba(239,68,68,.1)  !important;border-color:rgba(239,68,68,.3)  !important}}
body.dark .kpi-green {{background:rgba(34,197,94,.1)  !important;border-color:rgba(34,197,94,.3)  !important}}
.kpi-label{{font-size:.72rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px}}
.kpi-blue   .kpi-label{{color:#3b82f6}}.kpi-blue   .kpi-value{{color:#3b82f6}}
.kpi-green  .kpi-label{{color:#22c55e}}.kpi-green  .kpi-value{{color:#22c55e}}
.kpi-orange .kpi-label{{color:#f97316}}.kpi-orange .kpi-value{{color:#f97316}}
.kpi-purple .kpi-label{{color:#8b5cf6}}.kpi-purple .kpi-value{{color:#8b5cf6}}
.kpi-teal   .kpi-label{{color:#0d9488}}.kpi-teal   .kpi-value{{color:#0d9488}}
.kpi-red    .kpi-label{{color:#ef4444}}.kpi-red    .kpi-value{{color:#ef4444}}
.kpi-value{{font-family:'JetBrains Mono',monospace;font-size:2.6rem;font-weight:700;line-height:1}}
.kpi-sub{{font-size:.75rem;color:#94a3b8;margin-top:6px;line-height:1.4}}

/* ── section label ── */
.sl{{font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#94a3b8;margin-bottom:12px}}

/* ── cards ── */
.card{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
body.dark .card{{background:#1e293b;border-color:#334155}}

/* ── 2-col grid ── */
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}}

/* ── tables ── */
table{{width:100%;border-collapse:collapse;font-size:.82rem}}
th{{font-size:.72rem;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:#94a3b8;border-bottom:1px solid #f1f5f9;padding:6px 10px;text-align:left}}
body.dark th{{border-color:#334155}}
td{{padding:8px 10px;border-bottom:1px solid #f8fafc;vertical-align:middle}}
body.dark td{{border-color:#1e293b}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#f8fafc}}
body.dark tr:hover td{{background:#0f172a}}

/* ── tags ── */
.tag{{display:inline-block;padding:2px 8px;border-radius:6px;font-size:.7rem;font-weight:600;font-family:'JetBrains Mono',monospace}}
.tv{{background:#dcfce7;color:#166534;border:1px solid #bbf7d0}}
.tv2{{background:#ffedd5;color:#9a3412;border:1px solid #fed7aa}}
.tn{{background:#dbeafe;color:#1e40af;border:1px solid #bfdbfe}}
.tw{{background:#f3e8ff;color:#6b21a8;border:1px solid #e9d5ff}}
.ta{{background:#fef9c3;color:#854d0e;border:1px solid #fef08a}}
body.dark .tv{{background:rgba(34,197,94,.15);color:#4ade80;border-color:rgba(34,197,94,.3)}}
body.dark .tv2{{background:rgba(249,115,22,.15);color:#fb923c;border-color:rgba(249,115,22,.3)}}
body.dark .tn{{background:rgba(59,130,246,.15);color:#60a5fa;border-color:rgba(59,130,246,.3)}}
body.dark .tw{{background:rgba(139,92,246,.15);color:#a78bfa;border-color:rgba(139,92,246,.3)}}
body.dark .ta{{background:rgba(234,179,8,.15);color:#facc15;border-color:rgba(234,179,8,.3)}}

/* ── callsign badge ── */
.cs{{display:inline-block;font-family:'JetBrains Mono',monospace;font-size:.75rem;font-weight:600;background:rgba(13,148,136,.1);color:#0d9488;border:1px solid rgba(13,148,136,.2);padding:2px 8px;border-radius:6px}}
body.dark .cs{{background:rgba(45,212,191,.1);color:#2dd4bf;border-color:rgba(45,212,191,.2)}}

/* ── bar charts ── */
.br{{display:flex;align-items:center;gap:10px;margin:7px 0}}
.bl{{min-width:86px;font-size:.8rem;font-weight:500;color:#475569;flex-shrink:0}}
body.dark .bl{{color:#94a3b8}}
.bc{{min-width:30px;text-align:right;font-size:.8rem;font-weight:700;font-family:'JetBrains Mono',monospace;color:#0f172a;flex-shrink:0}}
body.dark .bc{{color:#f1f5f9}}
.bt{{flex:1;background:#f1f5f9;border-radius:4px;height:10px;overflow:hidden}}
body.dark .bt{{background:#334155}}
.bf{{height:10px;border-radius:4px;transition:width .4s cubic-bezier(.4,0,.2,1)}}

/* ── events ── */
.ev{{border-radius:10px;padding:12px 16px;margin-bottom:10px;font-size:.8rem;line-height:1.55;border-left:3px solid transparent}}
.ev-warn{{background:#fff7ed;border-left-color:#f97316}}
.ev-ok{{background:#f0fdf4;border-left-color:#22c55e}}
.ev-info{{background:#eff6ff;border-left-color:#3b82f6}}
body.dark .ev-warn{{background:rgba(249,115,22,.08);border-left-color:#f97316}}
body.dark .ev-ok{{background:rgba(34,197,94,.08);border-left-color:#22c55e}}
body.dark .ev-info{{background:rgba(59,130,246,.08);border-left-color:#3b82f6}}
.ev-t{{font-weight:600;margin-bottom:3px;color:#0f172a}}
.ev-warn .ev-t{{color:#c2410c}}
.ev-ok  .ev-t{{color:#15803d}}
.ev-info .ev-t{{color:#1d4ed8}}
body.dark .ev-t{{color:#f1f5f9}}
body.dark .ev-warn .ev-t{{color:#fb923c}}
body.dark .ev-ok  .ev-t{{color:#4ade80}}
body.dark .ev-info .ev-t{{color:#60a5fa}}
.ev-b{{color:#64748b;font-size:.78rem}}
body.dark .ev-b{{color:#94a3b8}}

/* ── map ── */
.map-wrap{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;overflow:hidden;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
body.dark .map-wrap{{background:#1e293b;border-color:#334155}}
.map-tb{{display:flex;align-items:center;gap:8px;padding:12px 16px;border-bottom:1px solid #f1f5f9;flex-wrap:wrap}}
body.dark .map-tb{{border-color:#334155}}
.map-tb .sl{{margin:0;flex:1}}
.fb{{cursor:pointer;padding:5px 14px;border-radius:20px;border:1px solid #e2e8f0;background:#f8fafc;color:#475569;font-family:inherit;font-size:.75rem;font-weight:600;transition:all .15s}}
.fb:hover{{border-color:#cbd5e1}}
body.dark .fb{{background:#334155;border-color:#475569;color:#94a3b8}}
.fa   {{background:#3b82f6;border-color:#3b82f6;color:#fff}}
.fb2  {{background:#3b82f6;border-color:#3b82f6;color:#fff}}
.fb-guest{{background:#3b82f6;border-color:#3b82f6;color:#fff}}
.fb-part {{background:#f97316;border-color:#f97316;color:#fff}}
.fb-gw   {{background:#8b5cf6;border-color:#8b5cf6;color:#fff}}
#map{{height:360px;width:100%}}
.map-wrap.expanded{{position:fixed;top:0;left:0;right:0;bottom:0;z-index:9998;margin:0;border-radius:0;display:flex;flex-direction:column}}
.map-wrap.expanded #map{{flex:1;height:auto}}
.map-wrap.expanded .map-tb,.map-wrap.expanded .map-leg{{flex-shrink:0}}
.map-leg{{display:flex;gap:16px;padding:10px 16px;border-top:1px solid #f1f5f9;flex-wrap:wrap;background:#f8fafc}}
body.dark .map-leg{{background:#0f172a;border-color:#334155}}
.li{{display:flex;align-items:center;gap:6px;font-size:.74rem;color:#64748b}}
body.dark .li{{color:#94a3b8}}
.ld{{width:11px;height:11px;border-radius:50%;flex-shrink:0}}

/* ── refresh notice ── */
.rfx{{display:flex;align-items:center;gap:12px;padding:10px 16px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;margin-bottom:20px;font-size:.82rem;flex-wrap:wrap}}
body.dark .rfx{{background:rgba(34,197,94,.07);border-color:rgba(34,197,94,.2)}}
.rfx-status{{color:#374151;flex:1}}
body.dark .rfx-status{{color:#94a3b8}}
.rfx-reload{{font-family:'Inter',sans-serif;font-size:.82rem;font-weight:600;color:#fff;background:#0d9488;border:none;border-radius:8px;padding:6px 16px;cursor:pointer;white-space:nowrap}}
.rfx-reload:hover{{background:#0f766e}}
.rfx-hint{{font-size:.75rem;color:#6b7280;white-space:nowrap}}
body.dark .rfx-hint{{color:#6b7280}}
.rfx-hint code{{background:rgba(0,0,0,.08);border-radius:4px;padding:1px 5px;font-family:'JetBrains Mono',monospace;font-size:.72rem}}
body.dark .rfx-hint code{{background:rgba(255,255,255,.08)}}


.df-wrap{{display:flex;align-items:center;gap:8px;margin-bottom:20px;flex-wrap:wrap}}
.df-label{{font-size:.75rem;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-right:4px}}
body.dark .df-label{{color:#94a3b8}}
.df-sel{{font-family:'Inter',sans-serif;font-size:.82rem;font-weight:500;color:#1e293b;background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:6px 12px;cursor:pointer;box-shadow:0 1px 2px rgba(0,0,0,.05)}}
body.dark .df-sel{{background:#1e293b;border-color:#334155;color:#e2e8f0}}
.df-custom{{display:none;align-items:center;gap:6px}}
.df-custom.show{{display:flex}}
.df-inp{{font-family:'Inter',sans-serif;font-size:.82rem;color:#1e293b;background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:5px 10px;box-shadow:0 1px 2px rgba(0,0,0,.05)}}
body.dark .df-inp{{background:#1e293b;border-color:#334155;color:#e2e8f0}}
.df-sep{{color:#94a3b8;font-size:.8rem}}
.df-apply{{font-family:'Inter',sans-serif;font-size:.78rem;font-weight:600;color:#fff;background:#0d9488;border:none;border-radius:8px;padding:6px 14px;cursor:pointer}}
.df-range{{font-size:.75rem;color:#94a3b8;margin-left:8px}}


/* ── sortable table headers ── */
th.sort{{cursor:pointer;user-select:none;white-space:nowrap;transition:color .15s}}
th.sort:hover{{color:#0d9488}}
body.dark th.sort:hover{{color:#2dd4bf}}
th.sort::after{{content:' ⇅';opacity:.3;font-size:.75em}}
th.sort.asc::after{{content:' ↑';opacity:1;color:#0d9488}}
th.sort.desc::after{{content:' ↓';opacity:1;color:#0d9488}}
body.dark th.sort.asc::after,body.dark th.sort.desc::after{{color:#2dd4bf}}

.ft{{text-align:center;color:#94a3b8;font-size:.72rem;padding:14px 0 4px;border-top:1px solid #e2e8f0;margin-top:8px}}
body.dark .ft{{border-color:#334155}}

/* ── responsive ── */
@media(max-width:1200px){{.kpi-row{{grid-template-columns:repeat(3,1fr)}}}}
@media(max-width:900px){{.kpi-row{{grid-template-columns:1fr 1fr}}.g2{{grid-template-columns:1fr}}}}
@media(max-width:500px){{.kpi-row{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="wrap">

<div class="hdr">
  <div class="hdr-left">
    <div class="hdr-icon">📡</div>
    <div>
      <div class="hdr-title">{HOME_CALL}-8 BBS &amp; Winlink Gateway &nbsp;
        <span class="accent">Log Analysis</span>
      </div>
      <div class="hdr-sub">
        BPQ32 v6.0.25.1 &nbsp;&middot;&nbsp;
        <span class="mono">{HOME_CALL} ({HOME_GRID})</span> &nbsp;&middot;&nbsp;
        {LOCATION} &nbsp;&middot;&nbsp;
        Operator: <span class="mono">{OP_CALL}</span> (Jason) &nbsp;&middot;&nbsp;
        {dlabel} &nbsp;&middot;&nbsp; Generated {now}
      </div>
    </div>
  </div>
  <button class="tog" onclick="toggleTheme()"><span id="ti">🌙</span><span id="tl">Dark mode</span></button>
</div>

<div class="rfx">
  <span style="font-size:1.1rem">⟳</span>
  <span class="rfx-status">Last generated: <strong>{now}</strong></span>
  <button class="rfx-reload" onclick="location.reload(true)" title="Reload page to pick up newly generated data">
    ↺ Reload
  </button>
  <span class="rfx-hint">Run <code>refresh.bat</code> first, then click Reload</span>
</div>

<div class="df-wrap">
  <span class="df-label">Date range</span>
  <select class="df-sel" id="df-sel" onchange="applyPreset(this.value)">
    <option value="all">All data</option>
    <option value="today" selected>Today</option>
    <option value="yesterday">Yesterday</option>
    <option value="week">Last 7 days</option>
    <option value="month">This month</option>
    <option value="year">This year</option>
    <option value="custom">Custom range&hellip;</option>
  </select>
  <div class="df-custom" id="df-custom">
    <input class="df-inp" type="date" id="df-from">
    <span class="df-sep">to</span>
    <input class="df-inp" type="date" id="df-to">
    <button class="df-apply" onclick="applyCustom()">Apply</button>
  </div>
  <span class="df-range" id="df-range"></span>
</div>

<div class="kpi-row">
  <div class="kpi kpi-green">
    <div class="kpi-label">Inbound BBS Connects</div>
    <div class="kpi-value" id="kv-inbound">{s.inbound_total}</div>
    <div class="kpi-sub">Total connection events to BBS ports</div>
  </div>
  <div class="kpi kpi-blue">
    <div class="kpi-label">Guest BBS Users</div>
    <div class="kpi-value" id="kv-bbs" data-total="{n_bbs_callers}">{n_bbs_callers}</div>
    <div class="kpi-sub">Humans browsing / using your BBS</div>
  </div>
  <div class="kpi kpi-orange">
    <div class="kpi-label">Partner BBS Users</div>
    <div class="kpi-value" id="kv-fwd" data-total="{n_inbound_forwarders}">{n_inbound_forwarders}</div>
    <div class="kpi-sub">Peer BBSs exchanging mail with you</div>
  </div>
  <div class="kpi kpi-purple">
    <div class="kpi-label">Winlink Gateway Users</div>
    <div class="kpi-value" id="kv-gw" data-total="{n_gw_users}">{n_gw_users}</div>
    <div class="kpi-sub">Stations using N4SFL-10 as RMS</div>
  </div>
  <div class="kpi {'kpi-red' if n_crashes else 'kpi-green'}" id="kpi-crashes-card">
    <div class="kpi-label">Crashes / Restarts</div>
    <div class="kpi-value" id="kv-crashes">{n_crashes}</div>
    <div class="kpi-sub" id="kv-crashes-sub">{"MiniDump present \u2014 review" if n_crashes else "No crashes detected"}</div>
  </div>
</div>

<div class="map-wrap">
  <div class="map-tb">
    <span class="sl">Station map</span>
    <button class="fb fa"        id="ba" onclick="setF('all')">All stations</button>
    <button class="fb"           id="bb" onclick="setF('guest')">Guest BBS</button>
    <button class="fb"           id="bc" onclick="setF('partner')">Partner BBS</button>
    <button class="fb"           id="bd" onclick="setF('gw')">Gateway</button>
    <button class="fb"           id="map-exp-btn" onclick="toggleMapExpand()" style="margin-left:auto">&#x26F6; Expand</button>
  </div>
  <div id="map"></div>
  <div class="map-leg">
    <div class="li"><div class="ld" style="background:#e05a00;border:2px solid #ff9f43"></div>Home ({HOME_CALL}-8)</div>
    <div class="li"><div class="ld" style="background:#3b82f6;border:2px solid #93c5fd"></div>Guest BBS User</div>
    <div class="li"><div class="ld" style="background:#f97316;border:2px solid #fed7aa"></div>Partner BBS User</div>
    <div class="li"><div class="ld" style="background:#8b5cf6;border:2px solid #c4b5fd"></div>Winlink Gateway User</div>
    <div class="li"><div class="ld" style="background:#64748b;border:2px solid #cbd5e1"></div>Multiple roles</div>
    <div class="li" style="margin-left:auto;font-style:italic">Locations via QRZ XML API &middot; click markers for details</div>
  </div>
</div>

<div class="g2">
  <div class="card">
    <div class="sl">Activity by day</div>
    <table>
    <table id="daily-table">
      <tr>
        <th class="sort asc" data-col="0" onclick="sortDaily(this)">Date</th>
        <th class="sort" data-col="1" onclick="sortDaily(this)">CMS Polls</th>
        <th class="sort" data-col="2" onclick="sortDaily(this)">Inbound</th>
        <th class="sort" data-col="3" onclick="sortDaily(this)">Messages</th>
        <th class="sort" data-col="4" onclick="sortDaily(this)">Unique</th>
      </tr>
      <tbody id="daily-tbody">{daily_rows}</tbody>
    </table>
    <div style="margin-top:18px">
      <div class="sl">Inbound connects by mode</div>
      {mode_bars}
    </div>
  </div>
  <div class="card">
    <div class="sl">Notable events &amp; issues</div>
    <div id="events-container"></div>
  </div>
</div>

<div class="g2" style="align-items:stretch">
  <div class="card" style="display:flex;flex-direction:column">
    <div class="sl">Guest BBS users</div>
    <div style="font-size:.75rem;color:#94a3b8;margin-bottom:10px">Humans connecting to browse or use the BBS &mdash; no B2 mail forwarding protocol detected</div>
    <div id="caller-rows" style="flex:1">{caller_rows}</div>
  </div>
  <div class="card" style="display:flex;flex-direction:column">
    <div class="sl">Partner BBS users</div>
    <div style="font-size:.75rem;color:#94a3b8;margin-bottom:10px">Peer BBSs calling you to exchange mail via B2 protocol &mdash; msgs shown as rcvd / sent</div>
    <div style="flex:1">{inbound_fwd_rows}</div>
  </div>
</div>

<div class="card" style="margin-bottom:16px">
  <div class="sl">Outbound forwarding partner health</div>
  <div style="font-size:.75rem;color:#94a3b8;margin-bottom:12px">Your station calling out to peer BBSs &mdash; each bar represents one peer. Count is frequency-level attempts (BPQ cycles through all configured frequencies per session).</div>
  <div id="peer-container">{peer_rows}</div>
</div>

<div class="card" style="margin-bottom:16px">
  <div class="sl">BBS User Detail</div>
  <div style="font-size:.75rem;color:#94a3b8;margin-bottom:10px">All stations that connected to your BBS &mdash; Guest = human user, Partner = peer BBS forwarding mail</div>
  <table id="bbs-table">
    <tr>
      <th class="sort" data-col="0" onclick="sortBbs(this)">Callsign</th>
      <th class="sort" data-col="1" onclick="sortBbs(this)">Role</th>
      <th class="sort" data-col="2" onclick="sortBbs(this)">Location</th>
      <th class="sort" data-col="3" onclick="sortBbs(this)">Distance</th>
      <th class="sort" data-col="4" onclick="sortBbs(this)">Mode</th>
      <th class="sort" data-col="5" onclick="sortBbs(this)">Last Active</th>
      <th class="sort" data-col="6" onclick="sortBbs(this)">Connects</th>
      <th class="sort" data-col="7" onclick="sortBbs(this)">Messages</th>
      <th>Email</th>
      <th>Status</th>
    </tr>
    <tbody id="bbs-tbody">{bbs_table_rows}</tbody>
  </table>
</div>

<div class="card" style="margin-bottom:16px">
  <div class="sl">Winlink User Detail</div>
  <div style="font-size:.75rem;color:#94a3b8;margin-bottom:10px">External clients using {HOME_CALL}-10 as RMS</div>
  <table id="gw-table">
    <tr>
      <th class="sort" data-col="0" onclick="sortGw(this)">Callsign</th>
      <th class="sort" data-col="1" onclick="sortGw(this)">Location</th>
      <th class="sort" data-col="2" onclick="sortGw(this)">Distance</th>
      <th class="sort" data-col="3" onclick="sortGw(this)">Mode</th>
      <th class="sort" data-col="4" onclick="sortGw(this)">Client</th>
      <th class="sort" data-col="5" onclick="sortGw(this)">Sessions</th>
      <th class="sort" data-col="6" onclick="sortGw(this)">Messages</th>
      <th class="sort" data-col="7" onclick="sortGw(this)">Data</th>
      <th>Email</th>
      <th>Status</th>
    </tr>
    <tbody id="gw-tbody">{gw_rows}</tbody>
  </table>
</div>

<div class="ft">
  {HOME_CALL}-8 &nbsp;&middot;&nbsp; {OP_CALL} &nbsp;&middot;&nbsp; {LOCATION}
  &nbsp;&middot;&nbsp; {HOME_GRID} &nbsp;&middot;&nbsp; callsign locations via QRZ XML API
</div>

<!-- Email edit modal -->
<div id="em-modal" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.5);z-index:9999;align-items:center;justify-content:center">
  <div style="background:#fff;border-radius:14px;padding:28px 32px;min-width:340px;box-shadow:0 20px 60px rgba(0,0,0,.3)">
    <div style="font-weight:700;font-size:1rem;margin-bottom:4px">Edit email — <span id="em-call" style="color:#3b82f6;font-family:'JetBrains Mono',monospace"></span></div>
    <div style="font-size:.8rem;color:#64748b;margin-bottom:16px">Stored in database &middot; overrides QRZ data &middot; leave blank to remove</div>
    <input id="em-input" type="email" placeholder="callsign@example.com"
      style="width:100%;padding:10px 14px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:.95rem;
             outline:none;box-sizing:border-box;margin-bottom:10px"
      onfocus="this.style.borderColor='#3b82f6'" onblur="this.style.borderColor='#e2e8f0'">
    <div id="em-status" style="font-size:.8rem;min-height:1.2em;margin-bottom:14px"></div>
    <div style="display:flex;gap:10px;justify-content:flex-end">
      <button onclick="closeEmailModal()" style="padding:8px 18px;border:1px solid #e2e8f0;border-radius:8px;background:#fff;cursor:pointer;font-size:.9rem">Cancel</button>
      <button onclick="saveEmail()" style="padding:8px 22px;border:none;border-radius:8px;background:#3b82f6;color:#fff;cursor:pointer;font-size:.9rem;font-weight:600">Save</button>
    </div>
  </div>
</div>

</div>

<script>
const stations={stations_js};
const COL={{home:'#e05a00',guest:'#3b82f6',partner:'#f97316',gw:'#8b5cf6',multi:'#64748b'}};
const BDR={{home:'#ff9f43',guest:'#93c5fd',partner:'#fed7aa',gw:'#c4b5fd',multi:'#cbd5e1'}};
function mkIcon(t){{
  const sz=t==='home'?16:11;
  return L.divIcon({{
    html:`<div style="width:${{sz}}px;height:${{sz}}px;border-radius:50%;background:${{COL[t]}};border:2.5px solid ${{BDR[t]}};box-shadow:0 0 6px rgba(0,0,0,.25)"></div>`,
    iconSize:[sz,sz],iconAnchor:[sz/2,sz/2],className:''
  }});
}}
const map=L.map('map').setView([30,-60],3);
const TLIGHT='https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}.png';
const TDARK ='https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}.png';
const TATTR ='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>';
const TOPTS ={{maxZoom:19,subdomains:'abcd',attribution:TATTR}};
let tl=L.tileLayer(TLIGHT,TOPTS).addTo(map);
const mkrs=[];
stations.forEach(st=>{{
  const m=L.marker([st.lat,st.lng],{{icon:mkIcon(st.type)}});
  m.bindPopup(`<div style="font-family:Inter,sans-serif;min-width:170px;font-size:13px">
    <div style="font-weight:700;font-size:1em;color:${{COL[st.type]}}">${{st.call}}</div>
    <div style="font-family:'JetBrains Mono',monospace;font-size:.8em;color:#64748b;margin-top:1px">${{st.grid}}</div>
    <div style="font-size:.82em;margin-top:6px;line-height:1.55;color:#334155">${{st.info}}</div>
    ${{st.dates&&st.dates.length?`<div style="font-size:.72em;color:#94a3b8;margin-top:6px">Active: ${{st.dates.join(', ')}}</div>`:''}}</div>`);
  m.stype=st.type;
  m.stData=st;   // keep reference for date filtering
  m.addTo(map);mkrs.push(m);
}});
// ── Date filter ──────────────────────────────────────────────────────────────
const DAILY      = {daily_json};
const EVENTS     = {events_json};
const PEER_DAILY = {peer_daily_json};

// ── Peer bar renderer ─────────────────────────────────────────────────────────
function renderPeers(from, to, showAll) {{
  // Aggregate per-peer totals for the selected date range
  const totals = {{}};
  for(const [peer, days] of Object.entries(PEER_DAILY)) {{
    let att=0, suc=0;
    for(const [date, v] of Object.entries(days)) {{
      if(showAll || (date>=from && date<=to)) {{
        att+=v.attempts; suc+=v.successes;
      }}
    }}
    if(att>0) totals[peer]={{att,suc}};
  }}

  // Sort by attempts desc
  const sorted=Object.entries(totals).sort((a,b)=>b[1].att-a[1].att);
  if(sorted.length===0) {{
    document.getElementById('peer-container').innerHTML=
      '<div style="color:#94a3b8;font-size:.8rem;padding:8px 0">No forwarding activity in this date range.</div>';
    return;
  }}

  const modeClsMap={{'VARA HF':'tv','VARA FM':'tv2','AX.25':'ta','NETROM':'tn'}};
  const html=sorted.map(([peer,v])=>{{
    const pp=v.att>0?Math.round(v.suc/v.att*100):0;
    const bc=pp>=70?'#22c55e':(pp>=20?'#f59e0b':'#ef4444');
    const lc=pp>=70?'#22c55e':(pp>=20?'#f59e0b':'#ef4444');
    const fail=v.att-v.suc;
    // Get modes from PEER_DAILY entries that match the range
    const peerModes=new Set();
    if(PEER_DAILY[peer]){{
      Object.entries(PEER_DAILY[peer]).forEach(([date,dv])=>{{
        if(showAll||(date>=from&&date<=to)){{
          (dv.modes||[]).forEach(m=>peerModes.add(m));
        }}
      }});
    }}
    const modeTags=[...peerModes].sort().map(m=>
      `<span class='tag ${{modeClsMap[m]||'tw'}}'>${{m}}</span>`
    ).join(' ');
    return `<div style="margin:10px 0">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;flex-wrap:wrap;gap:4px">
        <span style="font-size:.85em;font-weight:600">${{peer}}</span>
        <span style="display:flex;align-items:center;gap:6px">${{modeTags}}
          <span style="font-size:.8em;color:${{lc}}">${{v.suc}} ok / ${{fail}} freq-attempts</span>
        </span>
      </div>
      <div class="bt" style="height:8px">
        <div class="bf" style="width:${{Math.max(2,pp)}}%;background:${{bc}}"></div>
      </div>
    </div>`;
  }}).join('');
  document.getElementById('peer-container').innerHTML=html;
}}
// ─────────────────────────────────────────────────────────────────────────────

// ── Activity by Day sort ──────────────────────────────────────────────────────
let _dailySortCol=0, _dailySortAsc=false;  // default: date descending (newest first)
function sortDaily(th) {{
  const col=parseInt(th.dataset.col);
  if(_dailySortCol===col){{_dailySortAsc=!_dailySortAsc;}}else{{_dailySortCol=col;_dailySortAsc=true;}}
  document.querySelectorAll('#daily-table th.sort').forEach(h=>h.classList.remove('asc','desc'));
  th.classList.add(_dailySortAsc?'asc':'desc');
  const tbody=document.getElementById('daily-tbody');
  const rows=Array.from(tbody.querySelectorAll('tr'));
  const num=col>0;  // all columns except Date are numeric
  rows.sort((a,b)=>{{
    const av=a.cells[col]?.dataset?.v??'';
    const bv=b.cells[col]?.dataset?.v??'';
    const cmp=num?(parseFloat(av)||0)-(parseFloat(bv)||0):av.localeCompare(bv);
    return _dailySortAsc?cmp:-cmp;
  }});
  rows.forEach(r=>tbody.appendChild(r));
}}

// ── Gateway table sort ────────────────────────────────────────────────────────
let _gwSortCol=-1,_gwSortAsc=true;
function sortGw(th){{
  const col=parseInt(th.dataset.col);
  if(_gwSortCol===col){{_gwSortAsc=!_gwSortAsc;}}else{{_gwSortCol=col;_gwSortAsc=true;}}
  document.querySelectorAll('#gw-table th.sort').forEach(h=>h.classList.remove('asc','desc'));
  th.classList.add(_gwSortAsc?'asc':'desc');
  const tbody=document.getElementById('gw-tbody');
  const rows=Array.from(tbody.querySelectorAll('tr'));
  const num=col===2||col===5||col===7;
  rows.sort((a,b)=>{{
    const av=a.cells[col]?.dataset?.v??a.cells[col]?.textContent??'';
    const bv=b.cells[col]?.dataset?.v??b.cells[col]?.textContent??'';
    const cmp=num?(parseFloat(av)||0)-(parseFloat(bv)||0):av.localeCompare(bv,undefined,{{sensitivity:'base'}});
    return _gwSortAsc?cmp:-cmp;
  }});
  rows.forEach(r=>tbody.appendChild(r));
}}

// ── BBS table sort ────────────────────────────────────────────────────────────
let _bbsSortCol=-1,_bbsSortAsc=true;
function sortBbs(th){{
  const col=parseInt(th.dataset.col);
  if(_bbsSortCol===col){{_bbsSortAsc=!_bbsSortAsc;}}else{{_bbsSortCol=col;_bbsSortAsc=true;}}
  document.querySelectorAll('#bbs-table th.sort').forEach(h=>h.classList.remove('asc','desc'));
  th.classList.add(_bbsSortAsc?'asc':'desc');
  const tbody=document.getElementById('bbs-tbody');
  const rows=Array.from(tbody.querySelectorAll('tr'));
  const num=col===3||col===6;  // Distance and Connects numeric; Last Active sorts as ISO text
  rows.sort((a,b)=>{{
    const av=a.cells[col]?.dataset?.v??a.cells[col]?.textContent??'';
    const bv=b.cells[col]?.dataset?.v??b.cells[col]?.textContent??'';
    const cmp=num?(parseFloat(av)||0)-(parseFloat(bv)||0):av.localeCompare(bv,undefined,{{sensitivity:'base'}});
    return _bbsSortAsc?cmp:-cmp;
  }});
  rows.forEach(r=>tbody.appendChild(r));
}}
// ─────────────────────────────────────────────────────────────────────────────

// ── Events renderer ───────────────────────────────────────────────────────────
// ── Single source of truth for all KPI updates ────────────────────────────────
function syncAllKpis(from, to, showAll) {{

  // ── Inbound connects (from DAILY data) ──
  let inbound=0;
  DAILY.forEach(row=>{{ if(showAll||(row.date>=from&&row.date<=to)) inbound+=row.inbound; }});
  document.getElementById('kv-inbound').textContent=inbound;

  // ── BBS guests and partners (count from BBS detail table — authoritative) ──
  let guests=0, partners=0;
  document.querySelectorAll('#bbs-tbody tr').forEach(tr=>{{
    if(tr.style.display==='none') return;
    const v=(tr.cells[1]?.dataset?.v||'').trim();
    if(v==='guest') guests++;
    else if(v==='partner') partners++;
  }});
  // Fallback: if table is empty but bar charts are present, count those
  if(guests===0&&partners===0){{
    document.querySelectorAll('.guest-bar').forEach(el=>{{
      if(el.style.display!=='none') guests++;
    }});
    document.querySelectorAll('.partner-bar').forEach(el=>{{
      if(el.style.display!=='none') partners++;
    }});
  }}
  document.getElementById('kv-bbs').textContent=guests;
  document.getElementById('kv-fwd').textContent=partners;

  // ── Gateway users (count visible table rows) ──
  let gw=0;
  document.querySelectorAll('#gw-tbody tr').forEach(tr=>{{
    if(tr.style.display!=='none') gw++;
  }});
  document.getElementById('kv-gw').textContent=gw;

  // ── Crashes (count crash-type events in range) ──
  const crashEvs=EVENTS.filter(e=>e.type==='crash'&&(showAll||!e.date||(e.date>=from&&e.date<=to)));
  const crashN=crashEvs.length;
  document.getElementById('kv-crashes').textContent=crashN;
  const card=document.getElementById('kpi-crashes-card');
  if(card){{
    card.className='kpi '+(crashN>0?'kpi-red':'kpi-green');
    const sub=document.getElementById('kv-crashes-sub');
    if(sub) sub.textContent=crashN>0?'MiniDump present — review':'No crashes detected';
  }}
}}
// ─────────────────────────────────────────────────────────────────────────────

function renderEvents(from, to, showAll) {{
  const c = document.getElementById('events-container');
  if(!c) return;
  const evs = EVENTS.filter(e=>{{
    if(showAll) return true;
    if(!e.date) return true;   // undated events always show (forwarding failures etc)
    return e.date>=from && e.date<=to;
  }});
  if(evs.length===0){{
    c.innerHTML="<div class='ev ev-ok'><div class='ev-t'>&#10003; No events in this date range</div></div>";
    return;
  }}
  c.innerHTML=evs.map(e=>{{
    const isCrash=e.type==='crash', isWarn=e.type==='warn', isInfo=e.type==='info';
    const cls=isCrash||isWarn?'ev-warn':isInfo?'ev-info':'ev-ok';
    const icon=isCrash||isWarn?'&#9888;':isInfo?'&#9733;':'&#10003;';
    return `<div class='ev ${{cls}}'>`+
           `<div class='ev-t'>${{icon}} ${{e.title}}</div>`+
           (e.body?`<div class='ev-b'>${{e.body}}</div>`:'')+
           `</div>`;
  }}).join('');
}}
renderEvents('','',true);
renderPeers('','',true);
filterGwTable('','',true);
filterBbsTable('','',true);
document.querySelectorAll('.guest-bar,.partner-bar').forEach(el=>{{el.style.display='';}});
syncAllKpis('','',true);

// ── Table row filters (DOM only — KPIs updated by syncAllKpis) ───────────────
function filterGwTable(from, to, showAll) {{
  document.querySelectorAll('#gw-tbody tr').forEach(tr=>{{
    const dates=(tr.dataset.dates||'').split(',').filter(Boolean);
    tr.style.display=(showAll||dates.length===0||dates.some(d=>d>=from&&d<=to))?'':'none';
  }});
}}
function filterBbsTable(from, to, showAll) {{
  document.querySelectorAll('#bbs-tbody tr').forEach(tr=>{{
    const dates=(tr.dataset.dates||'').split(',').filter(Boolean);
    tr.style.display=(showAll||dates.length===0||dates.some(d=>d>=from&&d<=to))?'':'none';
  }});
}}
// ─────────────────────────────────────────────────────────────────────────────

// Track current active date filter for map re-filtering
let _dfFrom = '', _dfTo = '', _dfAll = true;
// Track current map type filter
let _mf = 'all';

function isoLocal(d) {{
  // Returns YYYY-MM-DD in LOCAL time (not UTC — logs use local Windows time)
  const y=d.getFullYear(), m=String(d.getMonth()+1).padStart(2,'0'), day=String(d.getDate()).padStart(2,'0');
  return y+'-'+m+'-'+day;
}}
function isoToday() {{ return isoLocal(new Date()); }}
function isoMondayOfWeek() {{
  const d=new Date(); const day=d.getDay()||7;
  d.setDate(d.getDate()-day+1); return isoLocal(d);
}}
function isoFirstOfMonth() {{
  const d=new Date(); return isoLocal(new Date(d.getFullYear(),d.getMonth(),1));
}}
function isoFirstOfYear() {{
  return new Date().getFullYear()+'-01-01';
}}

function applyPreset(val) {{
  const cust = document.getElementById('df-custom');
  if(val==='custom'){{ cust.classList.add('show'); return; }}
  cust.classList.remove('show');
  const today = isoToday();
  let from='', to=today;
  if(val==='today')         {{ from=today; }}
  else if(val==='yesterday'){{ const d=new Date(); d.setDate(d.getDate()-1); from=to=isoLocal(d); }}
  else if(val==='week')     {{ const d=new Date(); d.setDate(d.getDate()-6); from=isoLocal(d); }}
  else if(val==='month')    {{ from=isoFirstOfMonth(); }}
  else if(val==='year')     {{ from=isoFirstOfYear(); }}
  filterByRange(from, to, val==='all');
}}

function applyCustom() {{
  const from=document.getElementById('df-from').value;
  const to  =document.getElementById('df-to').value;
  if(!from||!to){{ alert('Please enter both start and end dates.'); return; }}
  filterByRange(from, to, false);
}}

function stationActiveInRange(st, from, to, showAll) {{
  // Home station is always visible
  if(st.type==='home') return true;
  // No date restriction
  if(showAll) return true;
  // Station has no date info → show it (conservative)
  if(!st.dates||st.dates.length===0) return true;
  return st.dates.some(d => d>=from && d<=to);
}}

function filterByRange(from, to, showAll) {{
  _dfFrom=from; _dfTo=to; _dfAll=showAll;

  // 1. Filter daily activity table
  document.querySelectorAll('#daily-tbody tr').forEach(tr=>{{
    const d=tr.dataset.date;
    tr.style.display=(showAll||!d||(d>=from&&d<=to))?'':'none';
  }});

  // 2. Filter BBS detail rows
  document.querySelectorAll('#bbs-tbody tr').forEach(tr=>{{
    const dates=(tr.dataset.dates||'').split(',').filter(Boolean);
    tr.style.display=(showAll||dates.length===0||dates.some(d=>d>=from&&d<=to))?'':'none';
  }});

  // 3. Filter gateway rows
  document.querySelectorAll('#gw-tbody tr').forEach(tr=>{{
    const dates=(tr.dataset.dates||'').split(',').filter(Boolean);
    tr.style.display=(showAll||dates.length===0||dates.some(d=>d>=from&&d<=to))?'':'none';
  }});

  // 4. Filter bar chart rows (Guest BBS and Partner BBS sections)
  document.querySelectorAll('.guest-bar,.partner-bar').forEach(el=>{{
    const dates=(el.dataset.dates||'').split(',').filter(Boolean);
    el.style.display=(showAll||dates.length===0||dates.some(d=>d>=from&&d<=to))?'':'none';
  }});

  // 4. Render dynamic content
  renderEvents(from, to, showAll);
  renderPeers(from, to, showAll);

  // 5. Sync ALL KPIs from DOM state — must be last
  syncAllKpis(from, to, showAll);

  // 6. Map filter
  applyMapFilter();

  // 7. Range label
  const lbl=document.getElementById('df-range');
  lbl.textContent=showAll?'':(from===to?from:from+' \u2013 '+to);
}}

function applyMapFilter() {{
  const f=_mf;
  mkrs.forEach(m=>{{
    const t=m.stype;
    const typeOk=f==='all'
      ||(f==='guest'   &&(t==='guest'  ||t==='home'||t==='multi'))
      ||(f==='partner' &&(t==='partner'||t==='home'||t==='multi'))
      ||(f==='gw'      &&(t==='gw'     ||t==='home'||t==='multi'));
    const dateOk=stationActiveInRange(m.stData,_dfFrom,_dfTo,_dfAll);
    const show=typeOk&&dateOk;
    show?(map.hasLayer(m)||m.addTo(map)):(map.hasLayer(m)&&map.removeLayer(m));
  }});
}}

function toggleMapExpand(){{
  const w=document.querySelector('.map-wrap');
  const isExp=w.classList.toggle('expanded');
  document.getElementById('map-exp-btn').innerHTML=isExp?'&#10005; Close':'&#x26F6; Expand';
  document.body.style.overflow=isExp?'hidden':'';
  setTimeout(()=>map.invalidateSize(),200);
}}

function setF(f){{
  document.getElementById('ba').className='fb'+(f==='all'?     ' fa':'');
  document.getElementById('bb').className='fb'+(f==='guest'?   ' fb-guest':'');
  document.getElementById('bc').className='fb'+(f==='partner'? ' fb-part':'');
  document.getElementById('bd').className='fb'+(f==='gw'?      ' fb-gw':'');
  _mf=f;
  applyMapFilter();
}}
// ─────────────────────────────────────────────────────────────────────────────

function toggleTheme(){{
  const isDark=document.body.classList.toggle('dark');
  document.getElementById('ti').textContent=isDark?'☀️':'🌙';
  document.getElementById('tl').textContent=isDark?'Light mode':'Dark mode';
  map.removeLayer(tl);
  tl=L.tileLayer(isDark?TDARK:TLIGHT,TOPTS).addTo(map);
}}

// ── Email edit modal ──────────────────────────────────────────────────────────
const API = 'http://127.0.0.1:5999';
let _emailCall = '';

function editEmail(call, currentEmail) {{
  _emailCall = call;
  document.getElementById('em-call').textContent = call;
  document.getElementById('em-input').value = currentEmail || '';
  document.getElementById('em-modal').style.display = 'flex';
  document.getElementById('em-input').focus();
  document.getElementById('em-status').textContent = '';
}}

function closeEmailModal() {{
  document.getElementById('em-modal').style.display = 'none';
}}

async function saveEmail() {{
  const email = document.getElementById('em-input').value.trim();
  const stat  = document.getElementById('em-status');
  stat.textContent = 'Saving…';
  try {{
    const r = await fetch(API + '/api/email', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{call: _emailCall, email}})
    }});
    if(r.ok) {{
      stat.textContent = '✓ Saved — will show on next refresh';
      stat.style.color = '#22c55e';
      setTimeout(closeEmailModal, 1800);
    }} else {{
      stat.textContent = '✗ Save failed — is the dashboard server running?';
      stat.style.color = '#ef4444';
    }}
  }} catch(e) {{
    stat.textContent = '✗ Cannot reach server (http://127.0.0.1:5999) — open via refresh.bat';
    stat.style.color = '#ef4444';
  }}
}}

document.addEventListener('keydown', e => {{
  if(e.key === 'Escape') closeEmailModal();
  if(e.key === 'Enter' && document.getElementById('em-modal').style.display === 'flex') saveEmail();
}});
// ─────────────────────────────────────────────────────────────────────────────

// Apply newest-first sort on Activity by Day, then apply Today filter
sortDaily(document.querySelector('#daily-table th.sort[data-col="0"]'));
applyPreset('today');

// ── Auto-reload when dashboard server detects new log data ───────────────────
(function(){{
  let knownTs=0;
  async function poll(){{
    try{{
      const r=await fetch('/api/last-refresh');
      if(!r.ok) return;
      const d=await r.json();
      if(knownTs&&d.ts>knownTs) location.reload(true);
      knownTs=d.ts;
      const h=document.querySelector('.rfx-hint');
      if(h) h.innerHTML='Auto-refresh active &mdash; watching logs every {CHECK_INTERVAL}s';
    }}catch(e){{
      // Server not reachable (file opened directly) — silently ignore
    }}
  }}
  poll();
  setInterval(poll,15000);
}})();
</script>
</body></html>"""


def parse_bpq_date(s: str) -> str:
    """Parse BPQ32 last-connect format 'DD-Mon HH:MMZ' → ISO date YYYY-MM-DD."""
    import re as _re
    from datetime import date as _date
    m = _re.match(r"(\d{1,2})-([A-Za-z]{3})", s.strip())
    if not m:
        return ""
    day = int(m.group(1))
    mon = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
           "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}.get(m.group(2).lower(), 0)
    if not mon:
        return ""
    today = _date.today()
    # If the month hasn't happened yet this year it must be last year
    year  = today.year if (mon, day) <= (today.month, today.day) else today.year - 1
    try:
        return _date(year, mon, day).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def load_manual_bbs_users(script_dir: Path) -> dict:
    """
    Load manually specified BBS users from bbs_users.txt in the Dashboard folder.
    Format (one per line):
        CALLSIGN
        CALLSIGN  2025-12-02        (with optional ISO last-connect date)
        CALLSIGN  02-Dec 06:11Z     (or BPQ32 date format)
        # lines starting with # are comments
    """
    results = {}
    path = script_dir / "bbs_users.txt"
    if not path.exists():
        return results
    import re as _re
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        call = parts[0].upper()
        if not _re.match(r"^[A-Z0-9]{1,3}[0-9][A-Z]{1,4}(-\d+)?$", call):
            continue
        last_raw = " ".join(parts[1:]) if len(parts) > 1 else ""
        # Accept ISO format YYYY-MM-DD directly
        if _re.match(r"^\d{4}-\d{2}-\d{2}$", last_raw):
            last_iso = last_raw
        else:
            last_iso = parse_bpq_date(last_raw)
        results[call] = {"last_connect": last_raw, "last_iso": last_iso,
                         "home_bbs": "", "name": ""}
    if results:
        print(f"  Manual bbs_users.txt: {len(results)} users loaded")
    return results


def load_email_overrides(script_dir: Path) -> dict:
    """
    Load manual email addresses from emails.txt in the Dashboard folder.
    Format (one per line):
        CALLSIGN  email@example.com
        # lines starting with # are comments
    These override or supplement QRZ data.
    """
    overrides = {}
    path = script_dir / "emails.txt"
    if not path.exists():
        return overrides
    import re as _re
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            call  = parts[0].upper()
            email = parts[1].lower()
            if "@" in email:
                overrides[call] = email
    if overrides:
        print(f"  Email overrides loaded: {len(overrides)} from emails.txt")
    return overrides


def fetch_bbs_users(host: str = "127.0.0.1", port: int = 8010, token: str = "") -> dict:
    """Fetch BBS user list from BPQ32 web interface AJAX endpoint."""
    import urllib.request, urllib.parse as _up, re as _re
    results = {}
    valid = _re.compile(r"^[A-Z0-9]{1,3}[0-9][A-Z]{1,4}(-\d+)?$")

    def _post(path, data=b""):
        url = f"http://{host}:{port}/{path}"
        try:
            req = urllib.request.Request(url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=3) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception:
            return None

    def _get(path):
        url = f"http://{host}:{port}/{path}"
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception:
            return None

    # BPQ32 AJAX endpoints — try each until we get callsigns
    tok = token or ""
    candidates = [
        f"Mail/GetData?{tok}",
        f"Mail/UserList?{tok}",
        f"Mail/GetUsers?{tok}",
    ]
    raw = None
    for path in candidates:
        text = _post(path) or _get(path)
        if text:
            # Look for newline-separated callsign list
            lines = [l.strip() for l in text.replace('\r','').split('\n') if l.strip()]
            calls = [l for l in lines if valid.match(l.upper())]
            if calls:
                raw = calls
                print(f"  BPQ32 user list from: {path}")
                break
            # Alternatively try parsing as HTML table
            if "<table" in text.lower() and "<td" in text.lower():
                raw_html = text
                row_re  = _re.compile(r"<tr[^>]*>(.*?)</tr>", _re.I | _re.S)
                cell_re = _re.compile(r"<td[^>]*>(.*?)</td>", _re.I | _re.S)
                tag_re  = _re.compile(r"<[^>]+>")
                for row in row_re.finditer(raw_html):
                    cells = [tag_re.sub("", c.group(1)).strip()
                             for c in cell_re.finditer(row.group(1))]
                    if cells and valid.match(cells[0].upper()):
                        raw = raw or []
                        raw.append(cells[0].upper())
                if raw:
                    print(f"  BPQ32 user list (HTML) from: {path}")
                    break

    if not raw:
        print(f"  BPQ32 web interface: no user list found (manual bbs_users.txt is the fallback)")
        return results

    for call in raw:
        call = call.upper()
        if valid.match(call):
            results[call] = {"last_connect": "", "last_iso": "", "home_bbs": "", "name": ""}

    # Optionally fetch details for each user from UserDetails endpoint
    # (skip to avoid N slow requests — manual file covers the important ones)
    print(f"  BPQ32 BBS users fetched: {len(results)}")
    return results
    return results


# ─── HISTORY DATABASE ────────────────────────────────────────────────────────────
DB_FILE = "bpq_history.db"

def db_open(script_dir: Path):
    import sqlite3
    conn = sqlite3.connect(str(script_dir / DB_FILE))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS bbs_callers(
            call TEXT PRIMARY KEY, connects INTEGER DEFAULT 0,
            is_partner INTEGER DEFAULT 0, modes TEXT DEFAULT '', grid TEXT DEFAULT '',
            first_seen TEXT DEFAULT '', last_seen TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS bbs_msgs(
            call TEXT PRIMARY KEY, received INTEGER DEFAULT 0, sent INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS gateway_users(
            call TEXT PRIMARY KEY, sessions INTEGER DEFAULT 0,
            bytes_sent INTEGER DEFAULT 0, bytes_rcvd INTEGER DEFAULT 0,
            msgs INTEGER DEFAULT 0, grid TEXT DEFAULT '',
            client TEXT DEFAULT '', modes TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS station_dates(
            call TEXT, iso TEXT, PRIMARY KEY(call,iso));
        CREATE TABLE IF NOT EXISTS forward_peers(
            peer TEXT, iso TEXT, attempts INTEGER DEFAULT 0,
            successes INTEGER DEFAULT 0, modes TEXT DEFAULT '',
            PRIMARY KEY(peer,iso));
        CREATE TABLE IF NOT EXISTS crashes(
            iso TEXT, dt TEXT, startup INTEGER DEFAULT 0, PRIMARY KEY(iso,dt));
    """)
    # Migrate existing DB: add columns if they didn't exist yet
    for col, defval in [("first_seen", "''"), ("last_seen", "''")]:
        try:
            conn.execute(f"ALTER TABLE bbs_callers ADD COLUMN {col} TEXT DEFAULT {defval}")
        except Exception:
            pass  # column already exists
    conn.commit()
    return conn


def db_load(conn, s: Stats):
    """Load historical data from DB into Stats, filling gaps where log files are gone."""
    c = conn.cursor()
    for row in c.execute("SELECT * FROM bbs_callers"):
        call = row["call"]
        if call not in s.bbs_callers:
            s.bbs_callers[call] = {
                "connects": row["connects"], "grid": row["grid"] or "",
                "modes": set((row["modes"] or "").split(",")) - {""}
            }
        else:
            e = s.bbs_callers[call]
            e["connects"] = max(e["connects"], row["connects"])
            e["modes"].update((row["modes"] or "").split(",")); e["modes"].discard("")
            if not e.get("grid") and row["grid"]: e["grid"] = row["grid"]
        if row["is_partner"]: s.inbound_b2_calls.add(call)

    for row in c.execute("SELECT * FROM bbs_msgs"):
        call = row["call"]
        cur  = s.inbound_b2_msgs.get(call, {"received":0,"sent":0})
        s.inbound_b2_msgs[call] = {
            "received": max(cur["received"], row["received"]),
            "sent":     max(cur["sent"],     row["sent"])
        }

    for row in c.execute("SELECT * FROM gateway_users"):
        call = row["call"]
        if call not in s.gateway_users:
            s.gateway_users[call] = {
                "sessions": row["sessions"], "bytes_sent": row["bytes_sent"],
                "bytes_rcvd": row["bytes_rcvd"], "msgs": row["msgs"],
                "grid": row["grid"] or "", "client": row["client"] or "",
                "dates": [], "modes": set((row["modes"] or "").split(",")) - {""}
            }
        else:
            gv = s.gateway_users[call]
            gv["sessions"]   = max(gv["sessions"],   row["sessions"])
            gv["bytes_sent"] = max(gv["bytes_sent"], row["bytes_sent"])
            gv["bytes_rcvd"] = max(gv["bytes_rcvd"], row["bytes_rcvd"])
            gv["msgs"]       = max(gv["msgs"],        row["msgs"])
            if not gv.get("grid")   and row["grid"]:   gv["grid"]   = row["grid"]
            if not gv.get("client") and row["client"]: gv["client"] = row["client"]
            gv["modes"].update((row["modes"] or "").split(",")); gv["modes"].discard("")

    for row in c.execute("SELECT * FROM station_dates"):
        s.station_dates.setdefault(row["call"], set()).add(row["iso"])

    for row in c.execute("SELECT * FROM forward_peers"):
        peer = row["peer"]; iso = row["iso"]
        fp = s.forward_peers.setdefault(peer, {"attempts":0,"successes":0,"modes":set()})
        fp["attempts"]  = max(fp["attempts"],  row["attempts"])
        fp["successes"] = max(fp["successes"], row["successes"])
        fp["modes"].update((row["modes"] or "").split(",")); fp["modes"].discard("")
        s.forward_peers_daily.setdefault(peer, {}).setdefault(iso, {
            "attempts": row["attempts"], "successes": row["successes"],
            "modes": list((row["modes"] or "").split(","))
        })

    seen = {(cd["iso"],cd["dt"]) for cd in s.crash_dates}
    for row in c.execute("SELECT * FROM crashes"):
        key = (row["iso"], row["dt"])
        if key not in seen:
            s.crash_dates.append({"iso":row["iso"],"dt":row["dt"],"startup":bool(row["startup"])})
            seen.add(key)

    n = c.execute("SELECT COUNT(*) FROM station_dates").fetchone()[0]
    print(f"  History DB: {n} station-date records loaded")


def db_save(conn, s: Stats):
    """Write current Stats back to the database, merging with existing records."""
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    for call, cv in s.bbs_callers.items():
        is_p      = 1 if call in s.inbound_b2_calls else 0
        modes_str = ",".join(cv.get("modes", set()))
        dates     = sorted(s.station_dates.get(call, set()))
        last_seen = dates[-1] if dates else today
        c.execute("""INSERT INTO bbs_callers(call,connects,is_partner,modes,grid,first_seen,last_seen)
                     VALUES(?,?,?,?,?,?,?)
                     ON CONFLICT(call) DO UPDATE SET
                       connects=MAX(connects,excluded.connects),
                       is_partner=MAX(is_partner,excluded.is_partner),
                       modes=CASE WHEN excluded.modes!='' THEN excluded.modes ELSE modes END,
                       grid=CASE WHEN excluded.grid!='' THEN excluded.grid ELSE grid END,
                       first_seen=CASE WHEN first_seen='' THEN excluded.first_seen ELSE first_seen END,
                       last_seen=MAX(last_seen,excluded.last_seen)""",
                  (call, cv["connects"], is_p, modes_str, cv.get("grid",""),
                   today, last_seen))

    for call, md in s.inbound_b2_msgs.items():
        c.execute("""INSERT INTO bbs_msgs(call,received,sent) VALUES(?,?,?)
                     ON CONFLICT(call) DO UPDATE SET
                       received=MAX(received,excluded.received),
                       sent=MAX(sent,excluded.sent)""",
                  (call, md.get("received",0), md.get("sent",0)))

    for call, gv in s.gateway_users.items():
        c.execute("""INSERT INTO gateway_users(call,sessions,bytes_sent,bytes_rcvd,msgs,grid,client,modes)
                     VALUES(?,?,?,?,?,?,?,?)
                     ON CONFLICT(call) DO UPDATE SET
                       sessions=MAX(sessions,excluded.sessions),
                       bytes_sent=MAX(bytes_sent,excluded.bytes_sent),
                       bytes_rcvd=MAX(bytes_rcvd,excluded.bytes_rcvd),
                       msgs=MAX(msgs,excluded.msgs),
                       grid=CASE WHEN excluded.grid!='' THEN excluded.grid ELSE grid END,
                       client=CASE WHEN excluded.client!='' THEN excluded.client ELSE client END,
                       modes=CASE WHEN excluded.modes!='' THEN excluded.modes ELSE modes END""",
                  (call, gv["sessions"], gv["bytes_sent"], gv["bytes_rcvd"],
                   gv.get("msgs",0), gv.get("grid",""), gv.get("client",""),
                   ",".join(gv.get("modes",set()))))

    for call, dates in s.station_dates.items():
        for iso in dates:
            c.execute("INSERT OR IGNORE INTO station_dates(call,iso) VALUES(?,?)", (call,iso))

    for peer, days in s.forward_peers_daily.items():
        for iso, pv in days.items():
            c.execute("""INSERT INTO forward_peers(peer,iso,attempts,successes,modes) VALUES(?,?,?,?,?)
                         ON CONFLICT(peer,iso) DO UPDATE SET
                           attempts=MAX(attempts,excluded.attempts),
                           successes=MAX(successes,excluded.successes),
                           modes=CASE WHEN excluded.modes!='' THEN excluded.modes ELSE modes END""",
                      (peer, iso, pv["attempts"], pv["successes"], ",".join(pv.get("modes",[]))))

    for cd in s.crash_dates:
        c.execute("INSERT OR IGNORE INTO crashes(iso,dt,startup) VALUES(?,?,?)",
                  (cd["iso"], cd["dt"], 1 if cd.get("startup") else 0))

    conn.commit()
    n = c.execute("SELECT COUNT(*) FROM station_dates").fetchone()[0]
    print(f"  History DB: {n} station-date records saved")


def main():
    script_dir = Path(__file__).parent
    cfg_path   = script_dir / "bpq_dashboard.cfg"
    cfg_user, cfg_pass, cfg_token, cfg_manual_users = "", "", "", []
    if os.path.exists(cfg_path):
        cfg = configparser.ConfigParser()
        cfg.read(cfg_path)
        cfg_user  = cfg.get("qrz", "username", fallback="")
        cfg_pass  = cfg.get("qrz", "password", fallback="")
        cfg_token = cfg.get("bpq", "token", fallback="")
        # Manual BBS user list — comma-separated callsigns under [bbs_users] calls=
        cfg_manual_users = [c.strip().upper() for c in
                            cfg.get("bbs_users", "calls", fallback="").split(",")
                            if c.strip()]

    parser = argparse.ArgumentParser(description="BPQ32 log dashboard with QRZ callsign lookup")
    parser.add_argument("--days", type=int, default=DAYS_BACK,
                        help="Days of logs to include (0 = all, default: all)")
    parser.add_argument("--logdir",   default=LOG_DIR)
    parser.add_argument("--out",      default=OUT_FILE)
    parser.add_argument("--qrz-user", default=cfg_user)
    parser.add_argument("--qrz-pass", default=cfg_pass)
    parser.add_argument("--bpq-token", default=cfg_token,
                        help="BPQ32 web auth token (the M... part of your BPQ32 web URL)")
    parser.add_argument("--no-qrz",   action="store_true", help="Skip QRZ, use grids only")
    args = parser.parse_args()

    # Offer to save credentials on first use
    if args.qrz_user and args.qrz_pass and not os.path.exists(cfg_path):
        save = input(f"\nSave QRZ credentials to {cfg_path}? [y/N] ").strip().lower()
        if save == "y":
            cfg = configparser.ConfigParser()
            cfg["qrz"] = {"username": args.qrz_user, "password": args.qrz_pass}
            if args.bpq_token:
                cfg["bpq"] = {"token": args.bpq_token}
            with open(cfg_path, "w") as f:
                cfg.write(f)
            print(f"Saved to {cfg_path}")

    # BPQ32 web token — prompt interactively if not in config
    bpq_token = getattr(args, "bpq_token", "") or cfg_token
    if not bpq_token:
        print("\nBPQ32 web interface token not configured.")
        print("  Open http://127.0.0.1:8010/Mail/Users in your browser.")
        print("  Copy the part after the '?' in the URL (e.g. M000061557FE0)")
        tok = input("  Enter token (or press Enter to skip): ").strip()
        if tok:
            bpq_token = tok
            # Save to config
            cfg2 = configparser.ConfigParser()
            if os.path.exists(cfg_path):
                cfg2.read(cfg_path)
            if "bpq" not in cfg2:
                cfg2["bpq"] = {}
            cfg2["bpq"]["token"] = tok
            with open(cfg_path, "w") as f:
                cfg2.write(f)
            print(f"  Token saved to {cfg_path}")

    if not os.path.isdir(args.logdir):
        print(f"ERROR: Log directory not found: {args.logdir}")
        sys.exit(1)

    print(f"\nScanning logs: {args.logdir}  (last {args.days} day(s))")
    files = find_logs(args.logdir, args.days)
    for k, v in files.items():
        print(f"  {k:10s}: {len(v)} file(s)")

    s = Stats()
    print("\nParsing...")
    parse_debug(files["debug"], s)
    parse_cms_access(files["cms"], s)
    parse_connect_log(files["connect"], s)
    parse_bbs_log(files["bbs"], s)

    # Load history DB — adds data from log files that no longer exist on disk
    print("\nLoading history database...")
    db_conn = db_open(script_dir)
    # Snapshot who was known BEFORE this run — to detect new guests
    known_before = set(r[0] for r in db_conn.execute("SELECT call FROM bbs_callers WHERE is_partner=0"))
    db_load(db_conn, s)

    # Supplement with full BBS user list from BPQ32 web interface
    # This captures historical users whose log files no longer exist on disk
    print("\nFetching BBS user list from BPQ32...")
    bbs_web_users = fetch_bbs_users(token=bpq_token)
    if bbs_web_users:
        print(f"  Web users found: {sorted(bbs_web_users.keys())}")

    # Merge manually specified users from bbs_users.txt
    manual_users = load_manual_bbs_users(script_dir)
    for call, info in manual_users.items():
        if call not in bbs_web_users:
            bbs_web_users[call] = info

    # Merge manually listed callsigns from config [bbs_users] calls=
    if cfg_manual_users:
        added = 0
        for call in cfg_manual_users:
            if call and call not in s.bbs_callers:
                s.bbs_callers[call] = {"connects":0,"modes":set(),"grid":"",
                                       "last_connect":"manual","home_bbs":""}
                added += 1
        if added:
            print(f"  Manual BBS users added from config: {added}")
    for call, info in bbs_web_users.items():
        base = call
        last_iso = info.get("last_iso", "")
        if base not in s.bbs_callers:
            s.bbs_callers[base] = {"connects": 0, "modes": set(), "grid": "",
                                   "last_connect": info.get("last_connect",""),
                                   "home_bbs": info.get("home_bbs","")}
        else:
            if not s.bbs_callers[base].get("last_connect"):
                s.bbs_callers[base]["last_connect"] = info.get("last_connect","")
            s.bbs_callers[base]["home_bbs"] = info.get("home_bbs","")
        # Store the last connect date in station_dates so date filtering works
        if last_iso:
            s.station_dates.setdefault(base, set()).add(last_iso)

    # Scrub any non-callsign entries that leaked in from log parsers
    # Valid ham callsigns: prefix + digit + suffix, always contains at least one digit
    import re as _re
    _valid = _re.compile(r"^[A-Z0-9]{1,3}[0-9][A-Z]{1,4}(-\d+)?$")
    for d in [s.bbs_callers, s.gateway_users]:
        bad = [k for k in d if not _valid.match(strip_ssid(k))]
        for k in bad:
            del d[k]
            if bad:
                print(f"  Removed invalid callsign(s): {bad}")

    # Detect new guest BBS users (not seen in any previous run)
    for call in list(s.bbs_callers.keys()):
        base = strip_ssid(call)
        if base not in s.inbound_b2_calls and base not in known_before:
            s.new_bbs_guests.add(base)
    if s.new_bbs_guests:
        print(f"  New guest BBS users: {sorted(s.new_bbs_guests)}")

    all_calls = sorted({
        strip_ssid(c) for c in list(s.bbs_callers) + list(s.gateway_users)
        if not strip_ssid(c).startswith("N4SFL")
    })
    print(f"\nCallsigns to resolve: {len(all_calls)}")
    print(f"Grids from logs:      {len(s.grids)}")

    qrz = None
    if not args.no_qrz:
        if args.qrz_user and args.qrz_pass:
            print(f"\nQRZ: {args.qrz_user}")
            qrz = QRZClient(args.qrz_user, args.qrz_pass)
        else:
            print("\nNo QRZ credentials — grid fallback only.")
            print("Use --qrz-user / --qrz-pass or create bpq_dashboard.cfg")

    print("\nResolving locations...")
    geo = resolve_geo(all_calls, s, qrz)

    via_qrz  = sum(1 for v in geo.values() if v and v.get("source")=="qrz")
    via_grid = sum(1 for v in geo.values() if v and v.get("source")=="grid")
    resolved = via_qrz + via_grid
    print(f"\n  QRZ: {via_qrz}  grid: {via_grid}  unresolved: {len(all_calls)-resolved}")

    print("\nBuilding HTML...")
    # Load manually entered emails from DB
    email_overrides = {}
    try:
        import sqlite3 as _sq
        _conn = _sq.connect(str(script_dir / "bpq_history.db"))
        _conn.execute("CREATE TABLE IF NOT EXISTS emails(call TEXT PRIMARY KEY, email TEXT DEFAULT '')")
        for row in _conn.execute("SELECT call, email FROM emails WHERE email!=''"):
            email_overrides[row[0]] = row[1]
        _conn.close()
        if email_overrides:
            print(f"  DB emails loaded: {len(email_overrides)}")
    except Exception as e:
        print(f"  Could not load emails from DB: {e}")
    html = build_html(s, geo, args.days, email_overrides)

    # Save updated history to DB before writing HTML
    print("\nSaving history database...")
    db_save(db_conn, s)
    db_conn.close()
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅  {os.path.abspath(args.out)}")
    print(f"    CMS polls:    {s.cms_polls}")
    print(f"    Inbound:      {s.inbound_total}")
    print(f"    GW users:     {len(s.gateway_users)}")
    print(f"    BBS callers:  {len(s.bbs_callers)}")
    print(f"    Crashes:      {max(0,s.crashes-1)}")
    print(f"    Map stations: {resolved+1}")
    print(f"\n── Gateway users & dates ───────────────────────────")
    for call, gv in sorted(s.gateway_users.items()):
        dates = sorted(s.station_dates.get(call, set()))
        print(f"  {call:12s}  sessions={gv['sessions']}  dates={dates}")
    print(f"\n── BBS callers ─────────────────────────────────────")
    for call, cv in sorted(s.bbs_callers.items()):
        base  = strip_ssid(call)
        role  = "Partner" if base in s.inbound_b2_calls else "Guest"
        dates = sorted(s.station_dates.get(base, set()))
        print(f"  {call:12s}  [{role}]  connects={cv['connects']}  dates={dates}")


if __name__ == "__main__":
    _required = ['parse_debug','parse_cms_access','parse_connect_log','parse_bbs_log',
                 'parse_bpq_date','fetch_bbs_users','db_open','db_load','db_save','build_html']
    _missing = [f for f in _required if f not in globals()]
    if _missing:
        import sys as _sys
        print(f"FATAL: missing top-level functions: {_missing}")
        _sys.exit(1)
    main()
