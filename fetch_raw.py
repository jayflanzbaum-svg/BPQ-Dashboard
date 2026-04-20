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
import http.cookiejar
import urllib.request
from urllib.error import HTTPError, URLError


HOST = os.environ.get("BPQ_HOST", "127.0.0.1")
PORT = int(os.environ.get("BPQ_PORT", "8010"))
USER = os.environ.get("BPQ_SYSOP_USER", "")
PASS = os.environ.get("BPQ_SYSOP_PASS", "")

LOGIN_PAGE_RE = re.compile(r'type\s*=\s*["\']?password["\']?', re.IGNORECASE)
KEY_RE        = re.compile(r'/Mail/[A-Za-z]+\?(M[0-9A-Fa-f]+)')


def make_opener():
    """Build an HTTP opener with HTTP Basic Auth + cookie jar for the BPQ web interface.
    Cookies are needed so AJAX endpoints can ride the session opened by the parent page."""
    pwmgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    pwmgr.add_password(None, f"http://{HOST}:{PORT}/", USER, PASS)
    auth_handler   = urllib.request.HTTPBasicAuthHandler(pwmgr)
    cookie_handler = urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
    return urllib.request.build_opener(auth_handler, cookie_handler)


def is_login_page(text: str) -> bool:
    """BPQ returns an HTML login page when auth fails. Detect it case-insensitively."""
    return bool(LOGIN_PAGE_RE.search(text or ""))


def auto_detect_key(opener) -> str:
    """Find the rotating session key by hitting Mail pages directly.

    Strategy: GET several Mail entry points; on success BPQ either:
      (a) redirects to a URL like /Mail/Header?KEY (key in r.geturl()), or
      (b) returns HTML that contains /Mail/Xxx?KEY links we can regex.
    Try /Mail/Header, /Mail/Status, /Mail/Users in turn.
    """
    candidates = ["/Mail/Header", "/Mail/Status", "/Mail/Users", "/Node/NodeIndex.html", "/"]
    last_err = None
    for path in candidates:
        url = f"http://{HOST}:{PORT}{path}"
        try:
            with opener.open(url, timeout=5) as r:
                final_url = r.geturl()
                html = r.read().decode("utf-8", errors="replace")
        except Exception as e:
            last_err = f"{path}: {e}"
            continue
        if is_login_page(html):
            raise RuntimeError(
                f"{path} returned a login page — sysop credentials are wrong "
                f"(check USER= in bpq32.cfg Telnet port block)"
            )
        # (a) Key embedded in redirected URL
        m = KEY_RE.search(final_url)
        if m:
            print(f"  Key found in redirect: {final_url}")
            return m.group(1)
        # (b) Key embedded in returned HTML
        m = KEY_RE.search(html)
        if m:
            print(f"  Key found in HTML of: {path}")
            return m.group(1)
        last_err = f"{path}: no KEY in URL or body (first 200 chars: {html[:200]!r})"
    raise RuntimeError(f"No KEY found via any candidate page. Last error: {last_err}")


def fetch_and_dump(opener, path: str, key: str, out_file: str, method: str = "GET") -> None:
    """Fetch a /Mail/* endpoint with the session key, save to disk, print summary."""
    url = f"http://{HOST}:{PORT}{path}?{key}"
    print(f"\n--- {method} {url}")
    try:
        if method == "POST":
            req = urllib.request.Request(url, data=b"", method="POST")
        else:
            req = urllib.request.Request(url, method="GET")
        with opener.open(req, timeout=10) as r:
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

    # The /Mail/FWD and /Mail/Users pages are JS shells — fetch them to open the
    # session, then POST the AJAX endpoints they call to get the actual data.
    fetch_and_dump(opener, "/Mail/FWD",          key, "raw_fwd.html")
    fetch_and_dump(opener, "/Mail/FwdList.txt",  key, "raw_fwdlist.txt", method="POST")
    fetch_and_dump(opener, "/Mail/Users",        key, "raw_users.html")
    fetch_and_dump(opener, "/Mail/UserList.txt", key, "raw_userlist.txt", method="POST")

    print("\nDone. Inspect raw_fwdlist.txt and raw_userlist.txt — those have the data.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
