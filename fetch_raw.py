#!/usr/bin/env python3
"""
BPQ32 raw-fetch discovery script.

Pulls /Mail/FWD and /Mail/Users from the BPQ32 web interface using HTTP
Basic Auth with sysop credentials, auto-detecting the rotating session key.

Saves raw HTML responses to raw_fwd.html and raw_users.html for inspection.

Usage:
    set BPQ_SYSOP_USER=yoursysop
    set BPQ_SYSOP_PASS=yourpass
    python fetch_raw.py

Optional:
    set BPQ_HOST=127.0.0.1
    set BPQ_PORT=8010
"""

import os
import re
import sys
import urllib.request
from urllib.error import HTTPError, URLError


HOST = os.environ.get("BPQ_HOST", "127.0.0.1")
PORT = int(os.environ.get("BPQ_PORT", "8010"))
USER = os.environ.get("BPQ_SYSOP_USER", "")
PASS = os.environ.get("BPQ_SYSOP_PASS", "")

LOGIN_PAGE_RE = re.compile(r'type\s*=\s*["\']?password["\']?', re.IGNORECASE)
KEY_RE        = re.compile(r'/Mail/[A-Za-z]+\?(M[0-9A-Fa-f]+)')


def make_opener():
    """Build an HTTP opener with HTTP Basic Auth for the BPQ web interface."""
    pwmgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    pwmgr.add_password(None, f"http://{HOST}:{PORT}/", USER, PASS)
    handler = urllib.request.HTTPBasicAuthHandler(pwmgr)
    return urllib.request.build_opener(handler)


def is_login_page(text: str) -> bool:
    """BPQ returns an HTML login page when auth fails. Detect it case-insensitively."""
    return bool(LOGIN_PAGE_RE.search(text or ""))


def auto_detect_key(opener) -> str:
    """GET the BPQ root page and regex the session key from any /Mail/Foo?KEY link."""
    url = f"http://{HOST}:{PORT}/"
    with opener.open(url, timeout=5) as r:
        html = r.read().decode("utf-8", errors="replace")
    if is_login_page(html):
        raise RuntimeError("Root page returned a login page — sysop credentials likely wrong")
    m = KEY_RE.search(html)
    if not m:
        raise RuntimeError(
            f"No /Mail/Xxx?KEY link found in root page (first 500 chars):\n{html[:500]}"
        )
    return m.group(1)


def fetch_and_dump(opener, path: str, key: str, out_file: str) -> None:
    """Fetch a /Mail/* endpoint with the session key, save to disk, print summary."""
    url = f"http://{HOST}:{PORT}{path}?{key}"
    print(f"\n--- Fetching {url}")
    try:
        with opener.open(url, timeout=10) as r:
            status = r.status
            body   = r.read()
    except HTTPError as e:
        status = e.code
        body   = e.read() or b""
    except URLError as e:
        print(f"  URL error: {e}")
        return

    text = body.decode("utf-8", errors="replace")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(text)

    print(f"  Status: {status}")
    print(f"  Length: {len(body)} bytes")
    print(f"  Login page? {is_login_page(text)}")
    print(f"  Saved to: {out_file}")
    print(f"  First 500 chars:\n{text[:500]}")


def main() -> int:
    if not USER or not PASS:
        print("ERROR: set BPQ_SYSOP_USER and BPQ_SYSOP_PASS environment variables.")
        print("       Sysop credentials live under USER= lines in your Telnet port")
        print("       block in bpq32.cfg.")
        return 2

    print(f"Target: http://{HOST}:{PORT}/  (user: {USER})")

    opener = make_opener()

    print("\nAuto-detecting session key...")
    try:
        key = auto_detect_key(opener)
    except Exception as e:
        print(f"  FAILED: {e}")
        return 1
    print(f"  Key: {key}")

    fetch_and_dump(opener, "/Mail/FWD",   key, "raw_fwd.html")
    fetch_and_dump(opener, "/Mail/Users", key, "raw_users.html")

    print("\nDone. Inspect raw_fwd.html and raw_users.html before writing parsers.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
