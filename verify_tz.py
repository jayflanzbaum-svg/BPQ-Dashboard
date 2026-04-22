#!/usr/bin/env python3
"""
One-shot timezone verification for the BPQ32 DEBUG log.

Run on the BPQ32 Windows machine. Compares MiniDump file mtimes (ground
truth from the OS) to the timestamps parsed from DEBUG log lines, and
greps bpq32.cfg for any log-timezone configuration directives.

Usage:
    python verify_tz.py
"""

import os
import re
import glob
import datetime
from pathlib import Path

LOG_DIR = r"C:\Users\Jason\AppData\Roaming\BPQ32\Logs"
CFG_DIR = r"C:\Users\Jason\AppData\Roaming\BPQ32"

print("=" * 72)
print("CHECK 1 — MiniDump mtimes vs DEBUG log 'Program Starting' lines")
print("=" * 72)

# Find all MiniDump files in the log directory
dump_glob = os.path.join(LOG_DIR, "MiniDump*")
dumps = sorted(glob.glob(dump_glob), key=os.path.getmtime)
if not dumps:
    print(f"\nNo MiniDump files found in {LOG_DIR}")
    print("(Cannot verify timezone bug without crash dumps to compare against.)")
else:
    print(f"\nFound {len(dumps)} MiniDump file(s). Showing latest 5:\n")
    for d in dumps[-5:]:
        mt    = os.path.getmtime(d)
        local = datetime.datetime.fromtimestamp(mt)
        print(f"  {os.path.basename(d)}")
        print(f"    mtime (local from OS): {local.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"    Unix epoch:            {int(mt)}")

print()
print("DEBUG log 'Program Starting' lines (most recent 5):\n")
debug_re  = re.compile(r"^(\d{6})\s+(\d{2}:\d{2}:\d{2}).*Program Starting")
matches = []
for fp in sorted(glob.glob(os.path.join(LOG_DIR, "log_*_DEBUG.txt"))):
    try:
        with open(fp, encoding="utf-8", errors="replace") as f:
            for line in f:
                m = debug_re.match(line)
                if m:
                    matches.append((m.group(1), m.group(2), os.path.basename(fp)))
    except Exception as e:
        print(f"  (could not read {fp}: {e})")

for ymd, hms, fname in matches[-5:]:
    print(f"  {fname}: {ymd} {hms}  (parsed-as-local would render as same digits)")

print()
print("INTERPRETATION:")
print("  - If MiniDump mtimes are ~4 hours EARLIER than the matching")
print("    Program Starting timestamps, BPQ writes its log in UTC.")
print("    Example: mtime 2026-04-21 15:54  vs  log line '260421 19:54:17'")
print("  - If they match, log time is local — the symptom has a different cause.")
print()

print("=" * 72)
print("CHECK 2 — Search bpq32.cfg for log-timezone directives")
print("=" * 72)

cfg_path = os.path.join(CFG_DIR, "bpq32.cfg")
if not os.path.exists(cfg_path):
    # Try alternate location
    alt = Path(CFG_DIR) / "bpq32.cfg"
    if alt.exists():
        cfg_path = str(alt)

if not os.path.exists(cfg_path):
    print(f"\nbpq32.cfg not found at {cfg_path}")
    print("Try locating it manually with:  dir /s C:\\bpq32.cfg")
else:
    print(f"\nReading {cfg_path}")
    print("\nLines matching LOGTIMEZONE / LOGTIME / UTC / GMT / TIMEZONE / TZ:")
    pat = re.compile(r"\b(LOGTIMEZONE|LOGTIME|UTC|GMT|TIMEZONE|TZ)\b", re.IGNORECASE)
    found = False
    try:
        with open(cfg_path, encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f, 1):
                if pat.search(line):
                    print(f"  line {i:4d}: {line.rstrip()}")
                    found = True
        if not found:
            print("  (no matches — BPQ32 log timezone is hardwired, not configurable)")
    except Exception as e:
        print(f"  Error reading: {e}")

print()
print("=" * 72)
print("Done. Paste this entire output back to Claude.")
print("=" * 72)
