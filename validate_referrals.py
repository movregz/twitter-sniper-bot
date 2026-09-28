#!/usr/bin/env python3
"""
Validate Claude referral links — HIGH-SPEED API oracle (no browser rendering).

The referral page SPA is a static 126KB shell; the verdict is computed
client-side from the same endpoint this script calls directly:

    GET https://claude.ai/api/referral/code/<slug>
      → 200 {"code": slug, "campaign": "...", "is_valid": true|false}
      → 200 body `null`            (slug never existed → uncertain → reject)
      → 403 "Just a moment..."     (random Cloudflare challenge → retry)

Strict criteria (equivalent to the browser era, now on ground truth):
  valid  ⇔ is_valid is exactly true  (genuine, active, unexpired, unclaimed)
  reject ⇔ is_valid false (expired_or_used), body null (unknown_slug),
           challenge lost, non-JSON, or any error — never a blind 200-OK pass.
Test/demo slugs filtered as before.
"""

import json
import os
import re
import sys
import time

from curl_cffi.requests import Session as CFSession

# Persistent connection pool — one TCP+TLS handshake, reused for every check.
# Safe for concurrent use across worker threads (libcurl connection cache is
# mutex-guarded); drastically cuts per-request latency vs. a fresh cf_get.
_CF_SESSION = CFSession(impersonate="chrome")
_CF_SESSION.headers.update({"Accept": "application/json"})

# ── Campaign denylist: the oracle's is_valid:true is NOT sufficient ──────
# Proven false positive: claude_invite_contest links (2025 marketing contest,
# spammed ever since) return is_valid:true FOREVER — the campaign never had
# per-link use-tracking. Any campaign in the denylist is treated as zombie
# state → reject as blocked_campaign (definitive; gets stale-cached).
# Everything NOT on the list passes — so future Anthropic campaign formats
# flow through automatically. Extend via env SNIPER_CAMPAIGN_DENYLIST=c1,c2.
CAMPAIGN_DENYLIST = {
    c.strip() for c in
    os.environ.get(
        "SNIPER_CAMPAIGN_DENYLIST",
        "claude_invite_contest",
    ).split(",") if c.strip()
}

ORACLE_URL = "https://claude.ai/api/referral/code/{slug}"

SLUG_RE = re.compile(r"^https://claude\.ai/referral/([A-Za-z0-9_-]+)", re.IGNORECASE)

TEST_KEYWORDS = re.compile(
    r'TEST|test|demo|debug|example|sample|staging|dev-|test-|refmon|'
    r'monitoring|verify|check|placeholder|dummy|foo|bar|baz|sandbox|'
    r'trial|temporary|temp-',
    re.IGNORECASE,
)


def is_test_link(url):
    m = SLUG_RE.match(url)
    if not m:
        return True
    return bool(TEST_KEYWORDS.search(m.group(1)))


def check_url_fast(url, timeout=3.0, max_retries=2, impersonate="chrome"):
    """Synchronous core check — uses the persistent pooled session (no per-call
    handshake). Verdict logic identical; only transport is tuned for speed.

    Verdict reasons:
      ok              → VALID (send immediately)
      expired_or_used → definitive reject (is_valid strictly false)
      unknown_slug    → definitive reject (API body null)
      unparseable     → definitive reject (unexpected JSON shape)
      challenge_lost / rate_limit / http_5xx / transient / test → NOT a
        verdict; caller may requeue these (they don't assert validity)
    """
    m = SLUG_RE.match(url)
    if not m:
        return False, "test", "Not a claude.ai/referral link"
    slug = m.group(1)
    if TEST_KEYWORDS.search(slug):
        return False, "test", "Test/demo slug — rejected"

    last = "unknown"
    for attempt in range(max_retries + 1):
        t0 = time.monotonic()
        try:
            r = _CF_SESSION.get(
                ORACLE_URL.format(slug=slug),
                timeout=timeout,
            )
            dt = time.monotonic() - t0
            code = r.status_code

            if code == 403 or "just a moment" in r.text[:300].lower():
                last = "cf_challenge"
                if attempt < max_retries:
                    time.sleep(0.8 * (attempt + 1))
                    continue
                return False, "challenge_lost", f"CF challenge persisted ({dt:.2f}s/req)"
            if code == 429:
                last = "rate_limit"
                if attempt < max_retries:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                return False, "rate_limit", "API rate limit persisted"
            if code >= 500:
                last = f"http_{code}"
                if attempt < max_retries:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                return False, last, "Claude API 5xx persisted"
            if code != 200:
                return False, f"http_{code}", f"Unexpected status {code}"

            body = r.text.strip()
            if body in ("null", ""):
                return False, "unknown_slug", "API returned null — code never existed"
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                return False, "unparseable", f"Non-JSON body: {body[:120]!r}"
            if not isinstance(data, dict) or "is_valid" not in data:
                return False, "unparseable", f"Unexpected JSON shape: {body[:120]!r}"

            is_valid = data.get("is_valid")
            campaign = data.get("campaign", "?")
            if is_valid is True:
                if campaign in CAMPAIGN_DENYLIST:
                    return False, "blocked_campaign", (
                        f"is_valid=true but campaign '{campaign}' is denylisted "
                        f"(zombie campaign — definitive reject)")
                return True, "ok", f"ACTIVE referral confirmed (campaign={campaign}, {dt:.2f}s)"
            if is_valid is False:
                return False, "expired_or_used", f"is_valid=false (campaign={campaign}, {dt:.2f}s)"
            return False, "unclear", f"is_valid={is_valid!r} — uncertain, reject"

        except Exception as e:
            last = f"exception:{type(e).__name__}"
            if attempt < max_retries:
                time.sleep(0.8 * (attempt + 1))
                continue
            return False, "transient", f"Failed after retries: {last}"

    return False, "transient", f"Exhausted retries ({last})"


import asyncio


async def check_url_async(url, timeout=3.0, max_retries=2):
    """Async wrapper — sync check runs in a thread pool so a slow link never
    blocks the event loop. Identical strict verdict logic."""
    return await asyncio.get_running_loop().run_in_executor(
        None, lambda: check_url_fast(url, timeout=timeout, max_retries=max_retries)
    )


# ── Back-compat shim (old callers used check_url_via_browser) ────────────
def check_url_via_browser(url, timeout=8.0, max_retries=3, goto_wait=None):
    return check_url_fast(url, timeout=timeout, max_retries=max_retries)


# ── CLI for manual testing ───────────────────────────────────────────────
def main():
    urls = sys.argv[1:]
    out = {"valid_links": [], "checked": 0, "valid_count": 0, "results": []}
    for u in urls:
        valid, reason, detail = check_url_fast(u)
        out["results"].append({"url": u, "valid": valid, "reason": reason, "detail": detail})
        if valid:
            out["valid_links"].append(u)
        out["checked"] += 1
    out["valid_count"] = len(out["valid_links"])
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()