#!/bin/bash
# Daily state backup: snapshot + 7-day retention.
# Runs via sniper-backup.timer (systemd user unit) at midnight.
set -euo pipefail

DATA="$(cd "$(dirname "$0")" && pwd)/data"
STATE="$DATA/sniper_state.json"
STALE="$DATA/stale_slugs.json"
STAMP="$(date +%F)"
KEEP_DAYS=7

[ -f "$STATE" ] || { echo "no state file — nothing to back up" >&2; exit 0; }

cp "$STATE" "$DATA/sniper_state.$STAMP.json"
[ -f "$STALE" ] && cp "$STALE" "$DATA/stale_slugs.$STAMP.json"

# Rotate: drop day-stamped copies older than KEEP_DAYS
find "$DATA" -name 'sniper_state.*.json' -mtime +$KEEP_DAYS -delete
find "$DATA" -name 'stale_slugs.*.json' -mtime +$KEEP_DAYS -delete
