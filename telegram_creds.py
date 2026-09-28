#!/usr/bin/env python3
"""
Telegram credentials loader — reads from environment or an optional
local config file. NEVER hardcodes secrets.

Resolution order:
  1. Environment: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  2. Optional file: ~/.config/sniper/telegram.json
     {"TELEGRAM_BOT_TOKEN": "...", "TELEGRAM_CHAT_ID": "..."}

Create a bot with @BotFather to get a token; chat_id is your numeric
Telegram user/chat ID.
"""

import importlib.util
import json
import os
from pathlib import Path

_FALLBACK = Path.home() / ".config" / "sniper" / "telegram.json"


def load_telegram_creds():
    """Return (token, chat_id) from env or config file, or ('', '')."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if token and chat_id:
        return token, chat_id
    if _FALLBACK.exists():
        try:
            with open(_FALLBACK) as f:
                cfg = json.load(f)
            token = token or cfg.get("TELEGRAM_BOT_TOKEN", "")
            chat_id = chat_id or cfg.get("TELEGRAM_CHAT_ID", "")
        except (OSError, json.JSONDecodeError):
            pass
    return token, chat_id


# Backwards-compatible module attributes (the sniper imports these):
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID = load_telegram_creds()

if __name__ == "__main__":
    print("token loaded:", bool(TELEGRAM_BOT_TOKEN),
          "| chat_id loaded:", bool(TELEGRAM_CHAT_ID))
