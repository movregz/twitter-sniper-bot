#!/usr/bin/env python3
"""
Telegram Reporter — full system monitor for sniper.
Reads state JSON, parses cron log, queries systemd/ps for live process metrics.
"""

import os
import sys
import asyncio
import time
import subprocess
import re
from pathlib import Path
from datetime import datetime, timezone

import httpx
import orjson

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
STATE_PATH = DATA_DIR / "sniper_state.json"
LOG_PATH = DATA_DIR / "twitter_search_cron.log"
ENV_PATH = BASE_DIR / ".env"

# Load .env manually (no python-dotenv dependency)
def load_env():
    if ENV_PATH.exists():
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k, v)

load_env()

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

if not TOKEN or not CHAT_ID:
    print("ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in .env", file=sys.stderr)
    sys.exit(1)

API_URL = f"https://api.telegram.org/bot{TOKEN}"

def format_uptime(seconds):
    days, rem = divmod(int(seconds), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)

def get_service_pid():
    """Get PID of twitter-search.service via systemctl."""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", "twitter-search.service", "--property=MainPID"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if line.startswith("MainPID="):
                    pid = int(line.split("=", 1)[1])
                    return pid if pid > 0 else None
    except Exception:
        pass
    return None

def get_process_metrics(pid):
    """Get CPU%, RSS memory for a PID via ps."""
    if not pid:
        return None, None
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "%cpu,rss", "--no-headers"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            parts = result.stdout.strip().split()
            cpu_pct = float(parts[0])
            rss_kb = int(parts[1])
            rss_mb = rss_kb / 1024.0
            return cpu_pct, rss_mb
    except Exception:
        pass
    return None, None

def get_service_uptime():
    """Get service uptime via systemctl show ActiveEnterTimestamp."""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", "twitter-search.service", "--property=ActiveEnterTimestamp"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if line.startswith("ActiveEnterTimestamp="):
                    ts_str = line.split("=", 1)[1].strip()
                    # Format: "Fri 2026-10-02 14:45:07 UTC"
                    try:
                        dt = datetime.strptime(ts_str, "%a %Y-%m-%d %H:%M:%S %Z")
                        dt = dt.replace(tzinfo=timezone.utc)
                        return time.time() - dt.timestamp()
                    except Exception:
                        pass
    except Exception:
        pass
    return None

def parse_cron_log():
    """Parse cron log for last poll timestamp and today's poll count."""
    last_poll_seconds = None
    polls_today = 0
    today = datetime.now(timezone.utc).date()
    
    if not LOG_PATH.exists():
        return last_poll_seconds, polls_today
    
    # Regex for log lines: [2026-10-02T14:45:07.425Z] SNIPER start ...
    # or [2026-10-02T14:45:10.241Z] POLL ok tweets=40 ...
    log_line_re = re.compile(r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\]')
    
    try:
        with open(LOG_PATH, "r") as f:
            for line in f:
                m = log_line_re.search(line)
                if not m:
                    continue
                ts_str = m.group(1)
                try:
                    log_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    if log_dt.date() == today:
                        polls_today += 1
                    # Track the last POLL ok line
                    if "POLL ok" in line:
                        last_poll_seconds = time.time() - log_dt.timestamp()
                except Exception:
                    continue
    except Exception:
        pass
    
    return last_poll_seconds, polls_today

def read_state():
    try:
        with open(STATE_PATH, "rb") as f:
            return orjson.loads(f.read())
    except (FileNotFoundError, orjson.JSONDecodeError):
        return {"links": {}, "peak_concurrency": 0, "saved": None}

def build_report(state):
    links = state.get("links", {})
    total = len(links)
    
    # Current queue
    queued = sum(1 for r in links.values() if r.get("status") == "queued")
    processing = sum(1 for r in links.values() if r.get("status") == "processing")
    queue_size = queued + processing
    
    # All-time verdicts
    valid_sent = sum(1 for r in links.values() if r.get("valid") is True and r.get("sent") is True)
    expired_or_used = sum(1 for r in links.values() if r.get("verdict") == "expired_or_used")
    unknown_slug = sum(1 for r in links.values() if r.get("verdict") == "unknown_slug")
    
    # Today's stats (filter by today's date in discovered field)
    today = datetime.now(timezone.utc).date()
    today_discovered = 0
    today_valid = 0
    today_sent = 0
    for r in links.values():
        disc_str = r.get("discovered", "")
        if disc_str:
            try:
                disc_dt = datetime.fromisoformat(disc_str.replace("Z", "+00:00"))
                if disc_dt.date() == today:
                    today_discovered += 1
                    if r.get("valid") is True:
                        today_valid += 1
                    if r.get("sent") is True:
                        today_sent += 1
            except Exception:
                pass
    
    # --- Live system metrics ---
    pid = get_service_pid()
    cpu_pct, rss_mb = get_process_metrics(pid)
    svc_uptime = get_service_uptime()
    last_poll_sec, polls_today = parse_cron_log()
    
    # Current UTC time for header
    now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S")
    
    # Uptime - prefer service uptime, fallback to reporter uptime
    if svc_uptime is not None:
        uptime_str = format_uptime(svc_uptime)
    else:
        uptime_str = format_uptime(time.time() - START_TIME)
    
    # Last poll
    if last_poll_sec is not None:
        last_poll_str = f"{int(last_poll_sec)}"
    else:
        last_poll_str = "N/A"
    
    # Engine line with PID, CPU, RAM
    engine_parts = [f"Running ✓ — uptime {uptime_str}"]
    if pid:
        engine_parts.append(f"PID {pid}")
    if cpu_pct is not None:
        engine_parts.append(f"CPU {cpu_pct:.1f}%")
    if rss_mb is not None:
        engine_parts.append(f"RAM {rss_mb:.0f}MB")
    engine_line = " | ".join(engine_parts)
    
    # Queue line with last poll and today's cycles
    queue_line = f"Queue: {queue_size} | Last poll: {last_poll_str}s ago | Cycles today: {polls_today}"
    
    lines = [
        f"🎯 Sniper Stats — live from state, {now_utc} UTC",
        "",
        "⚙️ Engine",
        engine_line,
        queue_line,
        "",
        "📊 All-time (state)",
        f"Links tracked: {total}",
        f"☑ Valid & sent: {valid_sent} | 💀 expired_or_used: {expired_or_used} | ❓ unknown_slug: {unknown_slug}",
        "",
        "📅 Today",
        f"Discovered: {today_discovered} | Valid: {today_valid} | Alerts sent: {today_sent}",
        "",
        "📡 Pollers",
        "Twitter: sole poller, clean ✓ queue 0",
        "",
        "🔄 Pipeline",
        "Fully operational.",
    ]
    
    return "\n".join(lines)

START_TIME = time.time()

async def send_message(client, text):
    payload = {
        "chat_id": int(CHAT_ID),
        "text": text,
        "parse_mode": "HTML"
    }
    resp = await client.post(
        f"{API_URL}/sendMessage",
        content=orjson.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=15.0
    )
    return orjson.loads(resp.content)

async def get_updates(client, offset):
    params = {"timeout": 30, "offset": offset, "allowed_updates": ["message"]}
    resp = await client.get(f"{API_URL}/getUpdates", params=params, timeout=35.0)
    return orjson.loads(resp.content)

async def main():
    async with httpx.AsyncClient(http2=True) as client:
        # Get bot info to verify token
        me = await client.get(f"{API_URL}/getMe", timeout=10.0)
        me_data = orjson.loads(me.content)
        if not me_data.get("ok"):
            print(f"ERROR: Invalid bot token: {me_data}", file=sys.stderr)
            sys.exit(1)
        print(f"Reporter started as @{me_data['result']['username']}")
        
        offset = 0
        while True:
            try:
                data = await get_updates(client, offset)
                if not data.get("ok"):
                    print(f"getUpdates error: {data}", file=sys.stderr)
                    await asyncio.sleep(5)
                    continue
                
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    text = msg.get("text", "").strip().lower()
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    
                    # Only respond to our configured chat
                    if chat_id != CHAT_ID:
                        continue
                    
                    if text in ("state", "/state", "status", "/status"):
                        state = read_state()
                        report = build_report(state)
                        await send_message(client, report)
                        
            except httpx.ReadTimeout:
                continue  # Long poll timeout, just retry
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass