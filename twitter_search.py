#!/usr/bin/env python3
"""
Twitter/X Search — Claude Referral Links (Recent, Non-Test)

Searches Twitter (Top) for posts from the last 48 hours containing
https://claude.ai/referral/ links. Filters out TEST/debug links.
Extracts and outputs only the unique, non-test referral URLs.
"""

import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import curl_cffi
from curl_cffi.requests import Session

BEARER_TOKEN = (
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
    "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)

SEARCH_TIMELINE_QID = "Yw6L66Pw54NHKuq4Dp7b4Q"

DEFAULT_FEATURES = {
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "tweetypie_unmention_optimization_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "rweb_video_timestamps_enabled": True,
    "responsive_web_media_download_video_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "responsive_web_enhance_cards_enabled": False,
}

CLAUDE_REFERRAL_RE = re.compile(
    r'https://claude\.ai/referral/[A-Za-z0-9_-]+', re.IGNORECASE
)

# Keywords that indicate a test/demo/debug link — reject these
TEST_KEYWORDS = re.compile(
    r'TEST|test|demo|debug|example|sample|staging|dev-|\
test-|refmon|monitoring|verify|check|\
placeholder|dummy|foo|bar|baz|\
sandbox|trial|temporary|temp-',
    re.IGNORECASE
)


def load_cookies():
    auth_token = os.environ.get("TWITTER_AUTH_TOKEN", "")
    ct0 = os.environ.get("TWITTER_CT0", "")
    if not auth_token or not ct0:
        config_path = Path.home() / ".config" / "sniper" / "config.yaml"
        if config_path.exists():
            try:
                import yaml
                with open(config_path) as f:
                    config = yaml.safe_load(f)
                if not auth_token:
                    auth_token = config.get("TWITTER_AUTH_TOKEN", "")
                if not ct0:
                    ct0 = config.get("TWITTER_CT0", "")
            except Exception:
                pass
    if not auth_token or not ct0:
        raise RuntimeError("Missing Twitter cookies.")
    return auth_token, ct0


def _build_headers(ct0, auth_token):
    return {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Cookie": f"auth_token={auth_token}; ct0={ct0}",
        "X-Csrf-Token": ct0,
        "X-Twitter-Active-User": "yes",
        "X-Twitter-Auth-Type": "OAuth2Session",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Origin": "https://x.com",
        "Referer": "https://x.com/",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,en;q=0.8",
        "sec-ch-ua": '"Chromium";v="133", "Not(A:Brand";v="99", "Google Chrome";v="133"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }


def _deep_get(data, *keys):
    current = data
    for key in keys:
        if isinstance(key, int) and isinstance(current, list):
            if 0 <= key < len(current):
                current = current[key]
            else:
                return None
        elif isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return current


def _parse_single_tweet(result):
    if result.get("__typename") == "TweetTombstone":
        return None
    legacy = result.get("legacy", {})
    core = result.get("core", {})
    user_results = _deep_get(core, "user_results", "result") or {}
    user_core = user_results.get("core", {})
    user_legacy = user_results.get("legacy", {})
    screen_name = user_core.get("screen_name") or user_legacy.get("screen_name") or ""
    tweet_id = result.get("rest_id", "")
    note_text = _deep_get(result, "note_tweet", "note_tweet_results", "result", "text")
    text = note_text or legacy.get("full_text", "")
    urls = []
    for url_entry in _deep_get(legacy, "entities", "urls") or []:
        urls.append(url_entry.get("expanded_url", ""))
    # Include the tweet's created_at for recency filtering
    created_at = legacy.get("created_at", "")
    return {
        "id": tweet_id,
        "text": text,
        "username": "@" + screen_name if screen_name else "",
        "urls": urls,
        "created_at": created_at,
    }


def parse_search_response(data):
    tweets = []
    seen_ids = set()
    instructions = _deep_get(data, "data", "search_by_raw_query", "search_timeline", "timeline", "instructions")
    if not isinstance(instructions, list):
        return tweets
    for instruction in instructions:
        for entry in (instruction.get("entries") or []):
            content = entry.get("content", {})
            item_content = content.get("itemContent", {})
            if "tweet_results" not in item_content:
                continue
            result = item_content["tweet_results"].get("result")
            if not result:
                continue
            tweet = _parse_single_tweet(result)
            if tweet and tweet["id"] not in seen_ids:
                seen_ids.add(tweet["id"])
                tweets.append(tweet)
    return tweets


class TwitterSearchClient:
    def __init__(self, auth_token, ct0):
        self.session = Session(impersonate="chrome133a")
        self.headers = _build_headers(ct0, auth_token)

    def search(self, query, count=20, product="Top", since_hours=24):
        """Search with optional time filter (since_hours back from now)."""
        url = f"https://x.com/i/api/graphql/{SEARCH_TIMELINE_QID}/SearchTimeline"

        # Compute startTime in ISO 8601 format (Twitter expects: "YYYY-MM-DDTHH:MM:SSZ")
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=since_hours)
        start_time = since.strftime("%Y-%m-%dT%H:%M:%SZ")

        base_vars = {
            "count": min(count + 5, 40),
            "rawQuery": query,
            "querySource": "typed_query",
            "product": product,
            "startTime": start_time,
        }
        tweets = []
        seen_ids = set()
        cursor = None
        for _ in range(10):
            vars = dict(base_vars)
            if cursor:
                vars["cursor"] = cursor
            body = {"variables": vars, "queryId": SEARCH_TIMELINE_QID, "features": DEFAULT_FEATURES}
            resp = self.session.post(url, headers={**self.headers, "Content-Type": "application/json"}, json=body, timeout=30)
            if resp.status_code == 429:
                import time; time.sleep(15); continue
            if resp.status_code >= 400:
                raise RuntimeError(f"API error {resp.status_code}: {resp.text[:200]}")
            try:
                data = resp.json()
            except Exception:
                raise RuntimeError(f"Invalid JSON: {resp.text[:200]}")
            if data.get("errors"):
                code = data["errors"][0].get("code", 0)
                if code == 88:
                    import time; time.sleep(15); continue
                raise RuntimeError(f"Twitter API: {data['errors'][0].get('message', 'Unknown')}")
            new_tweets = parse_search_response(data)
            for t in new_tweets:
                if t["id"] and t["id"] not in seen_ids:
                    seen_ids.add(t["id"])
                    tweets.append(t)
            if len(tweets) >= count:
                break
            instructions = _deep_get(data, "data", "search_by_raw_query", "search_timeline", "timeline", "instructions")
            if not instructions:
                break
            last = instructions[-1].get("entries", [])
            if not last:
                break
            content = last[-1].get("content", {})
            if content.get("cursorType") == "Bottom":
                cursor = content.get("value")
            else:
                break
            import time; time.sleep(1.0)
        return tweets[:count]


def is_test_link(url):
    """Return True if the URL contains test/demo/debug keywords."""
    # Extract the slug part
    match = re.search(r'/referral/(.+)$', url)
    if not match:
        return True  # Bare referral root — reject
    slug = match.group(1).split('?')[0]  # Strip query params
    if TEST_KEYWORDS.search(slug):
        return True
    return False


def extract_referral_links(tweets):
    """Extract recent, non-test https://claude.ai/referral/<slug> links."""
    links = set()
    for tweet in tweets:
        for m in CLAUDE_REFERRAL_RE.finditer(tweet.get("text", "")):
            url = m.group(0)
            if url != "https://claude.ai/referral/" and not is_test_link(url):
                links.add(url)
        for url in tweet.get("urls", []):
            if url.startswith("https://claude.ai/referral/") and url != "https://claude.ai/referral/":
                if not is_test_link(url):
                    links.add(url)
    return sorted(links)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--since-hours", type=int, default=48, help="Only tweets from last N hours")
    args = parser.parse_args()

    auth_token, ct0 = load_cookies()
    client = TwitterSearchClient(auth_token, ct0)
    query = "claude.ai/referral OR claude referral link"
    tweets = client.search(query, count=args.count, product="Top", since_hours=args.since_hours)
    referral_links = extract_referral_links(tweets)

    if args.json:
        print(json.dumps({
            "referral_links": referral_links,
            "tweet_count": len(tweets),
            "since_hours": args.since_hours,
        }, indent=2, ensure_ascii=False))
    else:
        for link in referral_links:
            print(link)


if __name__ == "__main__":
    main()
