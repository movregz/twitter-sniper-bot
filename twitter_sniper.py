#!/usr/bin/env python3
"""
Claude Referral SNIPER — async event-driven monitor (v2, API-oracle based).

Flow: 32s Twitter poll (single reused curl_cffi session) → new link discovered
→ asyncio.Queue → up to 6 concurrent workers → strict is_valid oracle check
(~0.2s, 8s cap per attempt) → valid ⇒ Telegram send IMMEDIATELY (own await;
sibling workers keep validating) → state persisted on every change.

Latency instrumentation: DISCOVER / VSTART / VEND / SEND + total_discovery_to_send.
Single-instance: flock. Persistent dedup: sniper_state.json — a link that is
done or processing is never re-validated or re-sent.
"""

import asyncio
import fcntl
import importlib.util
import random
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone

import orjson
import httpx

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = BASE_DIR + "/data"

sys.path.insert(0, BASE_DIR)

import twitter_search as ts
import validate_referrals as vr

# ── Tunables ─────────────────────────────────────────────────────────────
POLL_INTERVAL = float(os.environ.get("SNIPER_POLL", "9.0"))    # final: guarantees
POLL_JITTER   = float(os.environ.get("SNIPER_POLL_JITTER", "0.5"))  # 8.5–9.5s, quota-safe
CONCURRENCY   = int(os.environ.get("SNIPER_WORKERS", "12"))   # oracle cheap
LINK_TIMEOUT  = float(os.environ.get("SNIPER_LINK_TIMEOUT", "2"))   # fail-fast
LINK_RETRIES  = int(os.environ.get("SNIPER_LINK_RETRIES", "2"))
CHALLENGE_GLOBAL = float(os.environ.get("SNIPER_CF_PAUSE", "20"))  # after 3 straight 403s
MAX_REQUEUE   = int(os.environ.get("SNIPER_MAX_REQUEUE", "40"))
DRY_RUN       = os.environ.get("SNIPER_DRY_RUN") == "1"

STATE_PATH = DATA_DIR + "/sniper_state.json"
LOG_PATH   = DATA_DIR + "/twitter_search_cron.log"
LOCK_PATH  = DATA_DIR + "/twitter_sniper.lock"

QUERY = "claude.ai/referral"  # A/B tested: URL-only = 34 links/poll vs 28 (keywords dilute the Latest window)

INFRA_REASONS = {"challenge_lost", "rate_limit", "transient"}  # requeue-able

# Reason → backoff base (s); backoff = base × attempts, cap 600
BACKOFF = {"challenge_lost": 20.0, "rate_limit": 30.0, "transient": 10.0}


LOG_MAX_BYTES = int(os.environ.get("SNIPER_LOG_MAX_BYTES", 5 * 1024 * 1024))  # 5 MB


def log(msg):
    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    # Size cap: rotate in place when over the limit. Keep the tail so recent
    # history survives. Same inode is preserved (truncate, not replace) so
    # systemd's StandardOutput=append: fd stays valid.
    try:
        if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > LOG_MAX_BYTES:
            with open(LOG_PATH, "rb") as f:
                f.seek(-LOG_MAX_BYTES // 2, os.SEEK_END)
                tail = f.read().split(b"\n", 1)[-1]
            with open(LOG_PATH, "wb") as f:  # truncate in place
                f.write(b"... [older log lines dropped by size cap] ...\n")
                f.write(tail)
    except Exception:
        pass  # never let log housekeeping break the monitor
    with open(LOG_PATH, "a") as f:
        f.write(f"[{ts_now}] {msg}\n")


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def mono():
    return time.monotonic()


_state_lock = asyncio.Lock()


# ── Stale-slug cache: definitive-reject memory shared by all pollers ─────
STALE_CACHE_PATH = DATA_DIR + "/stale_slugs.json"


def _slug_of(url):
    """https://claude.ai/referral/SLUG?... → slug (canonical key)."""
    m = re.match(r"https://claude\.ai/referral/([A-Za-z0-9_-]+)", url or "")
    return m.group(1) if m else url


class StaleCache:
    def __init__(self, path):
        self.path = path
        self.slugs = set()
        try:
            with open(path, "rb") as f:
                d = orjson.loads(f.read())
            self.slugs = set(d.get("slugs", []))
        except (FileNotFoundError, orjson.JSONDecodeError):
            pass

    def contains(self, url):
        return _slug_of(url) in self.slugs

    def add(self, url):
        s = _slug_of(url)
        if s not in self.slugs:
            self.slugs.add(s)
            self._flush()

    def _flush(self):
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            tmp = self.path + ".tmp"
            with open(tmp, "wb") as f:
                f.write(orjson.dumps(
                    {"slugs": sorted(self.slugs), "count": len(self.slugs)}))
            os.replace(tmp, self.path)
        except OSError:
            pass  # cache persistence is best-effort; never break the sniper


stale = StaleCache(STALE_CACHE_PATH)


# ── Persistent state (atomic writes, race-free) ──────────────────────────
class State:
    def __init__(self, path, links=None, peak_concurrency=0):
        self.path = path
        self.links = links or {}
        self.peak_concurrency = peak_concurrency
        self.active = 0
        self._flush()

    @classmethod
    def load(cls, path):
        links, peak = {}, 0
        try:
            with open(path, "rb") as f:
                d = orjson.loads(f.read())
            links = d.get("links", {})
            peak = d.get("peak_concurrency", 0)
        except (FileNotFoundError, orjson.JSONDecodeError):
            pass
        # Normalize legacy schema: v1 records used t_discovered_mono etc.
        for rec in links.values():
            if "t_disc" not in rec:
                rec["t_disc"] = mono()  # unknown true discovery time → now
        return cls(path, links, peak)

    def _flush(self):
        # Atomic write: temp file in the same directory → fsync → rename over
        # the target. os.replace is atomic on POSIX, so a crash mid-write can
        # never leave a truncated/half-written sniper_state.json behind.
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        doc = {"links": self.links, "peak_concurrency": self.peak_concurrency,
               "saved": now_iso()}
        fd, tmp = tempfile.mkstemp(prefix=".state_", dir=os.path.dirname(self.path))
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(orjson.dumps(doc))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    async def add(self, url, source="Twitter"):
        async with _state_lock:
            if url in self.links:
                return False
            self.links[url] = {"status": "queued", "attempts": 0,
                               "discovered": now_iso(), "t_disc": mono(),
                               "source": source}
            self._flush()
            return True

    async def claim(self, url):
        """Pick up a queued link. Returns t_disc, or None if busy/done."""
        async with _state_lock:
            rec = self.links.get(url)
            if not rec or rec["status"] != "queued":
                return None  # duplicate-queue safety: never double-validate
            rec["status"] = "processing"
            rec["vstart"] = now_iso()
            rec["t_vstart"] = mono()
            self.active += 1
            if self.active > self.peak_concurrency:
                self.peak_concurrency = self.active
            self._flush()
            return rec.get("t_disc")

    async def finish(self, url, verdict, valid, send_ok=None):
        async with _state_lock:
            rec = self.links.get(url)
            if not rec:
                return
            rec.update({"status": "done", "verdict": verdict, "valid": valid,
                        "vend": now_iso(), "closed": mono()})
            if send_ok is not None:
                rec["sent"] = send_ok
            self.active = max(0, self.active - 1)
            self._flush()

    async def requeue(self, url, reason):
        async with _state_lock:
            rec = self.links[url]
            rec["status"] = "queued"
            rec["attempts"] = rec.get("attempts", 0) + 1
            rec["last_reason"] = reason
            self.active = max(0, self.active - 1)
            self._flush()
            return rec["attempts"]


# ── Global metrics + start time (for /stats dashboard) ────────────────────
START_TIME = time.time()
METRICS = {
    "twitter_links": 0,   # total links fetched by the Twitter poller
    "sent_valid": 0,      # valid links delivered to Telegram
}


def bump(metric, n=1):
    """Thread-safe-ish counter bump (single event loop = no locking needed)."""
    METRICS[metric] = METRICS.get(metric, 0) + n


# ── Telegram credentials (env or ~/.config/sniper/telegram.json; never hardcoded) ──
import telegram_creds


def tg_conf():
    """Return (token, chat_id), failing fast and loudly if missing."""
    token, chat = telegram_creds.load_telegram_creds()
    if not token or not chat:
        raise RuntimeError(
            "Telegram credentials missing — set TELEGRAM_BOT_TOKEN and "
            "TELEGRAM_CHAT_ID in the environment (see .env.example)")
    return token, chat


# One global persistent AsyncClient: HTTP/2, keep-alive connection pool to
# api.telegram.org — the TLS handshake happens once, then every send reuses
# the open connection. No per-send client construction, no executor hop.
_TG_CLIENT: httpx.AsyncClient | None = None
# Strong refs to fire-and-forget send tasks — without them the GC may
# collect a task mid-send (asyncio docs: keep a reference to every task).
_BG_TASKS: set = set()


def tg_client():
    global _TG_CLIENT
    if _TG_CLIENT is None:
        _TG_CLIENT = httpx.AsyncClient(http2=True, timeout=httpx.Timeout(15.0))
    return _TG_CLIENT


async def send_link(url, source="Twitter"):
    """Send ONE link immediately. Returns (ok, dt).
    Bold header names the source platform; link in <code> for tap-to-copy."""
    if DRY_RUN:
        log(f"SEND_DRY {url} src={source} (dry-run — no HTTP)")
        return True, 0.0
    token, chat = tg_conf()
    text = f"<b>here your magic link referral from {source}</b>\n<code>{url}</code>"
    payload = {"chat_id": int(chat), "text": text, "parse_mode": "HTML"}
    t0 = mono()
    resp = await tg_client().post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        content=orjson.dumps(payload),
        headers={"Content-Type": "application/json"})
    dt = mono() - t0
    data = orjson.loads(resp.content)
    if not data.get("ok"):
        raise RuntimeError(data.get("description", "telegram error"))
    return True, dt


CHALLENGE_GLOBAL = float(os.environ.get("SNIPER_CF_PAUSE", "20"))


class ChallengeGate:
    """Global pause only during real challenge walls (3+ straight 403s)."""

    def __init__(self):
        self.streak = 0
        self.until = 0.0

    def note(self, was_challenge):
        if was_challenge:
            self.streak += 1
            if self.streak >= 3:
                self.until = max(self.until, mono() + CHALLENGE_GLOBAL)
                log(f"CHALLENGE_WALL pause {CHALLENGE_GLOBAL:.0f}s (streak={self.streak})")
        else:
            self.streak = 0

    async def wait(self):
        while mono() < self.until:
            await asyncio.sleep(0.5)


gate = ChallengeGate()


# ── One link, end to end: validate → immediate send ──────────────────────
async def process_link(state, q, url, source="Twitter"):
    while True:
        await gate.wait()               # global pause only during challenge walls
        t_disc = await state.claim(url) # duplicate-queue guard inside
        if t_disc is None:
            log(f"SKIP {url} (processing/done — duplicate-queue guard)")
            return
        lat_start = mono() - t_disc
        log(f"VSTART {url} from_discovery={lat_start:.3f}s")

        was_challenge = False
        try:
            valid, reason, detail = await vr.check_url_async(
                url, timeout=LINK_TIMEOUT, max_retries=LINK_RETRIES)
            was_challenge = reason in ("challenge_lost", "cf_challenge")
        except Exception as e:          # per-link error isolation
            valid, reason, detail = False, "transient", f"unexpected: {e}"

        gate.note(was_challenge)
        log(f"VEND {url} result={reason} valid={valid} detail={str(detail)[:110]}")

        # ── VALID → fire-and-forget dispatch: the worker does
        # NOT block on Telegram's API response. state.finish() runs first so
        # the link is adjudicated instantly; a background task performs the
        # send and records the result. Sibling workers keep validating.
        if valid:
            t_mark = mono()
            await state.finish(url, "ok", True, send_ok=None)
            total = t_mark - t_disc
            log(f"VEND_OK_DISPATCH {url} src={source} "
                f"discovery_to_adjudication={total:.3f}s — send now async")

            async def _send_bg():
                try:
                    ok, send_dt = await send_link(url, source)
                    log(f"SEND OK {url} src={source} send={send_dt:.2f}s "
                        f"total_discovery_to_send={total + send_dt:.2f}s")
                    bump("sent_valid")
                    async with _state_lock:
                        state.links[url]["sent"] = True
                        state._flush()
                except Exception as e:
                    log(f"SEND FAIL {url} err={type(e).__name__}:{e} "
                        f"→ marked sent=false; never resent this cycle")
                    async with _state_lock:
                        state.links[url]["sent"] = False
                        state._flush()
            t_bg = asyncio.create_task(_send_bg())
            _BG_TASKS.add(t_bg)
            t_bg.add_done_callback(_BG_TASKS.discard)
            return

        if reason in INFRA_REASONS:
            attempts = await state.requeue(url, reason)
            if attempts > MAX_REQUEUE:
                await state.finish(url, f"infra_gave_up:{reason}", False)
                log(f"GIVE_UP {url} after {attempts} attempts ({reason}) — never sent")
                return
            delay = min(BACKOFF.get(reason, 15.0) * attempts, 600.0)
            log(f"REQUEUE {url} reason={reason} attempt={attempts} retry_in={delay:.0f}s")
            await asyncio.sleep(delay)   # inside THIS task; siblings unaffected
            continue

        # definitive verdict: expired_or_used / unknown_slug / unparseable / test
        #                       / blocked_campaign (zombie-campaign allowlist reject)
        stale.add(url)   # never re-validate this slug again (survives restarts)
        await state.finish(url, reason, False)
        log(f"REJECT {url} verdict={reason} detail={str(detail)[:110]}")


async def worker(state, q, wid):
    while True:
        item = await q.get()
        url, source = item if isinstance(item, tuple) else (item, "Twitter")
        try:
            await process_link(state, q, url, source)
        except Exception as e:
            log(f"WORKER{wid} crash on {url}: {e} — requeued, worker survives")
            try:
                await state.requeue(url, "worker_crash")
                # requeue() only flips the state record back to "queued"; it
                # never re-inserts into this asyncio.Queue, so the link would
                # sit idle until the next restart. Put it back here (with its
                # source label preserved).
                q.put_nowait((url, source))
            except Exception:
                pass
        finally:
            q.task_done()


# ── Poller: frequent search, enqueue immediately ─────────────────────────
async def poller(state, q):
    auth_token, ct0 = ts.load_cookies()
    sess = ts.TwitterSearchClient(auth_token, ct0)  # reused across polls
    loop = asyncio.get_running_loop()
    log(f"SNIPER poller start poll={POLL_INTERVAL:.0f}s workers={CONCURRENCY} "
        f"oracle=api_referral_code dry_run={DRY_RUN}")

    while True:
        t0 = mono()
        try:
            tweets = await loop.run_in_executor(
                None, lambda: sess.search(QUERY, count=40, product="Latest", since_hours=6))
            links = ts.extract_referral_links(tweets)
            bump("twitter_links", len(links))
            new = 0
            for url in links:
                if stale.contains(url):
                    continue          # definitively dead — never re-hit the oracle
                if await state.add(url, "Twitter"):
                    new += 1
                    log(f"DISCOVER {url}")
                    q.put_nowait((url, "Twitter"))
            log(f"POLL ok tweets={len(tweets)} links={len(links)} new={new} "
                f"elapsed={mono() - t0:.2f}s queue={q.qsize()}")
        except Exception as e:
            log(f"POLL ERROR: {e}")
            await asyncio.sleep(30)
            continue
        await asyncio.sleep(max(0.5, POLL_INTERVAL + random.uniform(-POLL_JITTER, POLL_JITTER)
                                - (mono() - t0)))


# ── One poller:


def acquire_lock():
    os.makedirs(os.path.dirname(LOCK_PATH), exist_ok=True)
    fd = open(LOCK_PATH, "w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        log("SNIPER second instance detected — exiting (single-instance guard)")
        sys.exit(0)
    return fd


async def amain():
    lock_fd = acquire_lock()  # MUST keep the reference: fd closes → flock released
    state = State.load(STATE_PATH)

    q = asyncio.Queue()
    reopened = 0
    requeued_urls = []
    for url, rec in list(state.links.items()):
        if rec.get("status") == "processing":      # dead predecessor's mid-flight
            rec["status"] = "queued"
            reopened += 1
        if rec.get("status") == "queued":
            requeued_urls.append(url)
    if reopened:
        log(f"RESUME {reopened} link(s) left mid-flight by previous instance")
    state._flush()

    # resume with staggered first-attempts: unknown-state links re-check at
    # ~0.15s each; challenge walls handled by engine backoff — no pacing gate.
    # Source labels come from the state record so alerts stay accurate after
    # a restart (no silent relabeling to "Twitter").
    for u in requeued_urls:
        q.put_nowait((u, state.links[u].get("source", "Twitter")))

    log(f"SNIPER start known={len(state.links)} pending={q.qsize()} "
        f"peak_concurrency={state.peak_concurrency}")

    # ⚠ telegram_listener was removed because it used getUpdates on the same
    # bot token as the Hermes gateway, causing Telegram 409 Conflict errors.
    # Stats requests are now handled by Hermes reading sniper_state.json directly.
    # Do NOT add another getUpdates consumer on this token.
    workers = [asyncio.create_task(worker(state, q, i)) for i in range(CONCURRENCY)]
    poll = asyncio.create_task(poller(state, q))
    await asyncio.gather(poll, *workers)


def main():
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass
    finally:
        # close the shared Telegram pool cleanly so no socket warnings
        if _TG_CLIENT is not None:
            try:
                asyncio.run(_TG_CLIENT.aclose())
            except Exception:
                pass


if __name__ == "__main__":
    main()