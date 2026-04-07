# BPQ-Dashboard — Claude Code context

## What this project is
A Python script that parses BPQ32 packet radio log files and generates a standalone HTML analytics dashboard. It runs on a Windows machine that also runs BPQ32, a packet radio BBS and Winlink gateway node (callsign N4SFL, Delray Beach FL).

## Key files
| File | Purpose |
|---|---|
| `bpq_dashboard.py` | Main script — all parsing, geo lookup, HTML generation |
| `dashboard_server.py` | Minimal HTTP server on port 5999 — serves HTML, handles email API |
| `refresh.bat` | Windows one-click runner |
| `bbs_users.txt` | Manual BBS user list with optional last-connect dates |

## Files that exist locally but are NOT in the repo (gitignored)
- `bpq_dashboard.cfg` — QRZ credentials and BPQ32 token
- `qrz_cache.json` — cached QRZ XML API results
- `bpq_history.db` — SQLite history database
- `N4SFL_Dashboard.html` — generated output

## Architecture of bpq_dashboard.py
The script is one file. Key sections in order:
1. **CONFIG constants** — callsign, grid, lat/lng, log dir, Outlook URL
2. **QRZClient class** — XML API session, lookup, cache
3. **Stats class** — accumulates all parsed data across log files
4. **Log parsers** — `parse_debug`, `parse_cms_access`, `parse_connect_log`, `parse_bbs_log`
5. **Helper functions** — `fmt_time_12h`, `mode_tags`, `recency_cell`, `email_cell`, `pct`, `haversine_mi`, etc.
6. **build_html()** — generates the complete HTML page as a Python f-string (~1000 lines)
7. **Database functions** — `db_open`, `db_load`, `db_save`
8. **fetch_bbs_users()** — HTTP fetch from BPQ32 web interface
9. **main()** — orchestrates everything

## Critical known issue — function absorption bug
When editing this file, be extremely careful that function bodies do not accidentally get placed INSIDE a preceding function after a `return` statement. This has happened repeatedly with:
- `parse_debug` getting absorbed into `fmt_time_12h`
- `fetch_bbs_users` getting absorbed into `load_manual_bbs_users` or `load_email_overrides`
- `pct` getting absorbed into `email_cell` or `recency_cell`

After any edit, always verify all top-level functions exist:
```python
import ast, re
with open('bpq_dashboard.py') as f: src = f.read()
ast.parse(src)
fns = re.findall(r'^def (\w+)', src, re.MULTILINE)
print(fns)
```
Required top-level functions: `parse_debug`, `parse_cms_access`, `parse_connect_log`, `parse_bbs_log`, `parse_bpq_date`, `load_manual_bbs_users`, `load_email_overrides`, `fetch_bbs_users`, `email_cell`, `recency_cell`, `pct`, `db_open`, `db_load`, `db_save`, `build_html`, `main`

## HTML generation approach
`build_html()` returns one giant Python f-string containing the complete HTML page including inline CSS and JavaScript. All CSS uses `{{` and `}}` (double-braced) to escape the f-string. All JS uses `{{` and `}}` too. This means f-string expressions like `{variable}` work normally but any literal `{` or `}` in CSS/JS must be doubled.

## Color scheme (consistent across KPIs, map, tags)
| Role | Color |
|---|---|
| Guest BBS users | Blue `#3b82f6` |
| Partner BBS users | Orange `#f97316` |
| Winlink gateway users | Purple `#8b5cf6` |
| VARA HF mode tag | Green |
| VARA FM mode tag | Orange |
| Active status (≤30d) | Green `#22c55e` |
| Recent status (≤90d) | Amber `#f59e0b` |
| Inactive (≤365d) | Grey `#94a3b8` |
| Dormant (>365d) | Light grey `#cbd5e1` |

## Station types (map markers)
- `home` — N4SFL-8 (orange-red)
- `guest` — human BBS callers, no B2 forwarding (blue)
- `partner` — BBS peers using B2 forwarding protocol (orange)
- `gw` — Winlink gateway users via N4SFL-10 (purple)
- `multi` — appeared in multiple roles (slate)

## Log file formats
- `log_YYMMDD_BBS.txt` — BBS activity, lines prefixed with `YYMMDD HH:MM:SS`
- `CMSAccess_YYYYMMDD.log` — CMS/gateway sessions
- `ConnectLog_YYMMDD.log` — inbound connections with Mode field
- `log_YYMMDD_DEBUG.txt` — crashes (`Program Starting` line)

## Testing without the BPQ32 machine
You cannot run the full script in Codespaces (no log files, no BPQ32 web interface). You can:
- Run `python3 -c "import ast; ast.parse(open('bpq_dashboard.py').read()); print('OK')"` to syntax check
- Write unit tests for individual functions using mock data
- Edit and push — pull on the BPQ32 machine to test for real

## Deployment
1. Edit in Codespace
2. `git push`
3. On BPQ32 machine: `git pull` then `refresh.bat`

## BPQ32 node details
- Callsign: N4SFL, grid EL96XL, Delray Beach FL (26.46, -80.10)
- Operator: N8FLA (Jason)
- BPQ32 web interface: http://127.0.0.1:8010
- Dashboard server: http://127.0.0.1:5999
- Log directory: C:\Users\Jason\AppData\Roaming\BPQ32\Logs

## 73 de N8FLA
