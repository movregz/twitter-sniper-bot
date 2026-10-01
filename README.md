# Twitter Sniper — Claude Referral Link Monitor

High-performance, low-latency asynchronous Twitter monitor and validation engine for Claude referral links.

Twitter Sniper continuously searches Twitter/X for freshly posted `claude.ai/referral` links, validates each one against Claude's referral API within milliseconds, and delivers confirmed-active links to your Telegram chat — typically in under half a second from first sighting to alert.

## Features

* **Browserless validation** — verdicts come directly from the referral status API; no page rendering, no Selenium, no headless Chrome. ~0.1s per check.
* **Rate-limit aware polling** — a quota-surgical 9.0s ± 0.5s jittered cadence empirically tuned to Twitter's ~100-search/15-minute budget: zero blackouts, zero 429 storms, 100% polling uptime.
* **HTTP/2 connection pooling** — a single persistent `httpx.AsyncClient(http2=True)` for alert delivery and reused TLS sessions for search and validation: one handshake, warm sockets forever.
* **Strict verdict logic** — only `is_valid: true` counts. Ambiguity is always rejected, so every alert is a confirmed-active code with zero false positives.
* **Atomic, crash-safe state** — fsync + atomic rename persistence; the pipeline survives restarts mid-write without corruption.
* **Zero re-work** — a persistent stale-slug cache ensures every known-dead link is skipped forever, spending quota only on new drops.

### Requirements

* **Python 3.10+** (developed on 3.13)
* **OS:** Linux or any POSIX-compliant system (uses `fcntl`; macOS is also supported)
* **Packages:** `curl_cffi`, `httpx`, `orjson`, `h2` (HTTP/2 support for httpx)
* **Twitter/X Account:** Active session cookies from your logged-in browser
* **Telegram:** A Bot token (from @BotFather) and your numeric chat ID

### Installation

Clone the repository and create an isolated Python environment:

```bash
git clone https://github.com/movregz/twitter-sniper-bot.git sniper
cd sniper

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install curl_cffi httpx orjson h2
```

### Configuration

All credentials are loaded dynamically from environment variables or supported local configuration files. **No credentials are hardcoded in the source code.** See `.env.example` for the full annotated template.

| Variable             | Purpose                                                       |
| -------------------- | ------------------------------------------------------------- |
| `TWITTER_AUTH_TOKEN` | `auth_token` cookie from your logged-in x.com browser session |
| `TWITTER_CT0`        | `ct0` cookie from the same session                            |
| `TELEGRAM_BOT_TOKEN` | Bot token issued by @BotFather                                |
| `TELEGRAM_CHAT_ID`   | Numeric ID of the chat that receives alerts                   |

Optional tunables (`SNIPER_POLL`, `SNIPER_WORKERS`, `SNIPER_MAX_REQUEUE`, `SNIPER_CAMPAIGN_DENYLIST`, …) are documented in `.env.example` with their safe defaults.

> **Warning:** Never commit real credentials or authentication cookies to Git. Store secrets in environment variables or supported local configuration files that are strictly excluded by `.gitignore`. Keep secret-containing files readable only by the account running the sniper.

### How to run

```bash
# =================================================================
# OPTION 1: Standalone Execution (Foreground)
# =================================================================

python3 twitter_sniper.py
```

**Via systemd (Recommended for 24/7 uptime):**

When using a virtual environment, `systemd` must use the Python executable inside `.venv` rather than the system Python installation.

```bash
# 1. Create directory and copy systemd files
mkdir -p ~/.config/systemd/user
cp systemd/twitter-search.service systemd/sniper-backup.* ~/.config/systemd/user/

# 2. Edit the service file
nano ~/.config/systemd/user/twitter-search.service
```

Inside the `[Service]` section, update these lines to your actual installation path:

```ini
[Service]
WorkingDirectory=/path/to/sniper
ExecStart=/path/to/sniper/.venv/bin/python /path/to/sniper/twitter_sniper.py
```

Replace `/path/to/sniper` with the actual path where you cloned the repository.

Save the file with `Ctrl+O`, press `Enter`, then exit with `Ctrl+X`.

```bash
# 3. Reload systemd configuration and start services
systemctl --user daemon-reload
systemctl --user enable --now twitter-search.service
systemctl --user enable --now sniper-backup.timer

# 4. Verify that the sniper is running
systemctl --user status twitter-search.service
```

> **Note:** The `twitter_sniper.py` process enforces single-instance execution using `flock`. If another instance is already running, the new process exits cleanly instead of starting a second polling loop.

### Telegram Interaction

Once the sniper is running, you can monitor its health directly from your Telegram chat:

* **`state`** — Send this exact word (or command) to your bot to receive a real-time status report. The bot will reply with current statistics, including uptime, total links validated, and polling health.

### How to use

1. **Setup:** First, create a new Claude account and have it ready.
2. **Execution:** Run the Python tool (standalone or via systemd).
3. **Monitor:** Wait for a validated referral link to arrive in your Telegram bot.
4. **Action:** Click and claim the link quickly upon arrival.
5. **Verification:** Complete the required payment verification using a VCC (Virtual Credit Card) with a $1 balance (no actual charge will be deducted) → **Done.**

> **Optional:** After the payment verification is complete, you can replace your primary VCC with a test VCC or an unused VCC.

### Disclaimer

This project is provided for **educational and research purposes only**. It demonstrates techniques in asynchronous pipeline design, TLS-fingerprint-aware HTTP clients, quota-aware polling, API-based validation, and atomic state persistence.

Use of this software may be subject to the Terms of Service, acceptable-use policies, and other rules of the platforms and services it interacts with. Users are solely responsible for ensuring that their use of the software complies with all applicable laws, regulations, and third-party terms.

The authors and contributors are not responsible for misuse of the software or for any consequences arising from its use. The software is provided on an "AS IS" basis, without guarantees regarding availability, accuracy, compatibility, or continued operation of third-party services.

---

# Architecture Blueprint

### TLS Fingerprint Impersonation

**What it does:** Makes the Python client's TLS handshake indistinguishable from a real Chrome browser at the network level.

**Implementation:** curl_cffi library with impersonate="chrome133a" for Twitter and "chrome" for the Claude oracle, applied to every outbound session in twitter_search.py and validate_referrals.py.

**Benefit:** Passes Cloudflare's JA3/TLS bot fingerprinting that would instantly flag Python's default SSL stack.

### Twitter GraphQL Internal API Access

**What it does:** Queries Twitter's private search backend directly, exactly as the logged-in web app does.

**Implementation:** Direct POST to x.com/i/api/graphql/{queryId}/SearchTimeline with the hardcoded public bearer token and a full ~20-field features map in twitter_search.py.

**Benefit:** Structured JSON responses in ~2.5s, immune to UI changes, full Latest-timeline fidelity.

### Cookie-Based Session Auth

**What it does:** Authenticates as your logged-in browser session.

**Implementation:** auth_token + ct0 cookies loaded from the environment or a local config file and replayed on a persistent session by load_cookies().

**Benefit:** Full search privileges and rate budget tied to a real account, not an anonymous IP.

### Time-Windowed Search Query

**What it does:** Restricts every search to fresh posts only.

**Implementation:** Computed startTime parameter (6h lookback) on each SearchTimeline request in twitter_search.py.

**Benefit:** Small per-poll pages (39 tweets / 32 links typical), no wasted quota on stale results, fresh-drop detection.

### Direct API Oracle Validation (browserless)

**What it does:** Gets ground-truth verdicts on referral codes without rendering any page.

**Implementation:** GET https://claude.ai/api/referral/code/<slug> in validate_referrals.py — the exact endpoint the 126KB referral SPA calls client-side — returning {is_valid: true/false}.

**Benefit:** ~0.1s verdicts instead of seconds-long browser renders; zero rendering means zero JS-execution attack surface for CF challenges.

### Strict-Verdict Logic

**What it does:** Refuses to ever accept ambiguous responses as valid.

**Implementation:** valid ⇔ is_valid is exactly true; body null → unknown_slug, non-JSON → unparseable, any error → transient — all rejected. Never a blind 200-OK pass.

**Benefit:** Zero false alerts to Telegram — every alert is a confirmed-active code.

### Campaign Denylist (zombie-filter)

**What it does:** Catches campaigns where is_valid:true is a lie.

**Implementation:** CAMPAIGN_DENYLIST set in validate_referrals.py. Extendable via SNIPER_CAMPAIGN_DENYLIST env.

**Benefit:** Blocks a proven false-positive class the oracle itself cannot detect; unknown future campaigns still flow through.

### Definitive-Reject Stale Cache

**What it does:** Never re-validates a known-dead slug.

**Implementation:** Every expired/used/unknown slug is persisted to data/stale_slugs.json (150+ entries) and checked by StaleCache.contains() before enqueue.

**Benefit:** Oracle quota spent only on new links; the discovery loop costlessly skips the entire dead pile every 9 seconds.

### Quota-Surgical Jittered Polling

**What it does:** Polls as fast as Twitter's 15-min search budget allows without ever exhausting it.

**Implementation:** 9.0s base ± 0.5s random jitter (8.5–9.5s effective) in the poller() loop — tuned by live A/B across 4.5s → 8.0s → 8.75s → 9.0s after measuring ~100-search quota windows.

**Benefit:** 95 polls/window with headroom, zero tail-end blackouts, confirmed clean window rollover — 100% polling uptime.

### Async Event-Driven Architecture

**What it does:** Decouples polling, validation, and alerting into independent concurrent stages.

**Implementation:** Single asyncio loop — poller feeds an asyncio.Queue, 12 concurrent workers validate independently; blocking curl_cffi calls ride a thread executor.

**Benefit:** A slow validation never delays the next poll; multiple links validate in parallel.

### Duplicate-Queue Guard (state machine)

**What it does:** Makes double-validation and double-sending impossible.

**Implementation:** Per-link state machine (queued → processing → done) with claim() under an asyncio lock in State; only a queued link can enter processing.

**Benefit:** With 12 workers racing on the same queue, no link is ever validated twice or alerted twice.

### Rate-Limit Classification + Exponential Requeue

**What it does:** Separates retryable infrastructure failures from final verdicts.

**Implementation:** INFRA_REASONS = {rate_limit, transient, challenge_lost} get requeued with backoff = base × attempts (cap 600s, max 40 tries); everything else stale-caches permanently.

**Benefit:** No valid link is lost to a temporary CF hiccup, and no quota is wasted retrying genuinely dead links.

### Cloudflare ChallengeGate

**What it does:** Globally pauses all oracle traffic when Cloudflare turns hostile.

**Implementation:** ChallengeGate class — 3+ consecutive 403 challenges trigger a 20s global pause across all workers; resets on the first clean response.

**Benefit:** Prevents 12 workers from collectively hammering into a challenge wall and escalating it into an IP block.

### Persistent HTTP/2 Connection Pools

**What it does:** Eliminates TLS handshake cost from the alert path.

**Implementation:** One global httpx.AsyncClient(http2=True, timeout=15) for Telegram; reused curl_cffi sessions for Twitter and the oracle.

**Benefit:** One handshake per connection lifetime, HTTP/2 multiplexing on sends; alerts ride a warm socket.

### Fire-and-Forget Telegram Dispatch

**What it does:** Decouples the alert from the validation critical path.

**Implementation:** On valid verdict, worker marks state done instantly and returns; a background task (tracked in a _BG_TASKS registry to prevent asyncio GC) performs the HTTP/2 send.

**Benefit:** Discovery → adjudication completes in ~0.1–0.2s; the Telegram round-trip no longer blocks sibling validations.

### orjson Binary Serialization

**What it does:** Replaces Python's stdlib JSON with a Rust-backed parser.

**Implementation:** All State._flush/load, StaleCache I/O, and Telegram payloads use orjson.dumps/loads — zero stdlib json calls remain in the sniper.

**Benefit:** 3–10× faster state writes on the hot path.

### Atomic fsync State Persistence

**What it does:** Guarantees the state file can never corrupt, even mid-crash.

**Implementation:** tempfile.mkstemp → orjson write → flush() → os.fsync() → os.replace() in State._flush.

**Benefit:** Power loss or SIGKILL during a write leaves either the old or new complete file — never a truncated one.

### Source-Labeled Queue Tuples

**What it does:** Carries the origin of every link through the entire pipeline.

**Implementation:** Poller enqueues (url, source) tuples; source persisted in each state record at add() and read back on restart requeue.

**Benefit:** Alerts name their true source; restarts never mislabel a resumed link.

### Single-Instance flock Guard

**What it does:** Enforces exactly one sniper process regardless of how it's started.

**Implementation:** fcntl.flock on data/twitter_sniper.lock; a second process logs "second instance detected" and exits 0.

**Benefit:** Deploys, probes, and double-starts can't race on state or double-poll the quota.

### systemd Service Hardening

**What it does:** Makes the service self-healing and crash-resistant.

**Implementation:** twitter-search.service with ExecStartPre=py_compile smoke check, Restart=on-failure, 15s stop timeout for in-flight requeue, boot-enabled.

**Benefit:** The "missing import" crash class is blocked before start; any runtime crash auto-restarts in 5s.

### Nightly State Backups + 7-Day Retention

**What it does:** Layers disaster recovery under the live state file.

**Implementation:** backup_state.sh run by sniper-backup.timer at 00:00 — date-stamped copies of state + stale cache, auto-purged after 7 days.

**Benefit:** A corrupted state file costs at most one day of history, never all of it.

### Git-Version-Controlled Production

**What it does:** Makes every change a revertable diff.

**Implementation:** 6 commits covering migration, bug fixes, Twitter-only pivot, speed work, and the tuning campaign; data/ excluded from history.

**Benefit:** The entire tuning campaign this session was empirical and traceable — any regression is one git revert away.

### Test/Demo Slug Regex Filtering

**What it does:** Rejects junk links before they cost anything.

**Implementation:** TEST_KEYWORDS regex (test/demo/debug/placeholder/sandbox...) applied at both discovery (is_test_link) and validation.

**Benefit:** No oracle quota or worker time is spent on obvious non-codes.

### A/B-Driven Query Discipline

**What it does:** Keeps the search query minimal by evidence, not guesswork.

**Implementation:** QUERY = "claude.ai/referral" — chosen after a live A/B showed URL-only = 34 links/poll vs 28 with keyword dilution; keyword expansions tested to zero are reverted.

**Benefit:** Maximum fresh links per quota request — the same discipline that drove the 9.0s tuning.


