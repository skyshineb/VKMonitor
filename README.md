# VK Wall Monitor

Small production-ready monitor for one public VK group wall.

It polls VK `wall.get`, checks keywords (or regex), and sends instant Telegram messages for matching new posts.
State is stored in SQLite to avoid duplicate alerts after restart.

## Features

- Default polling interval: 30 seconds (safe for one group).
- VK target by either `VK_DOMAIN` or `VK_OWNER_ID`.
- Keyword modes: `any` (default) / `all` (case-insensitive for `KEYWORDS`).
- Optional regex mode (`KEYWORDS_REGEX`) with case-insensitive search.
- Searches in post text and `copy_history` text.
- Duplicate protection with SQLite in `run` mode:
`last_seen_post_id` per wall owner; `notified_posts` table to avoid re-sending same post.
- Telegram safety:
max 1 msg/sec local pacing; retries once on Telegram 429 using `retry_after`.
- Telegram inbound commands:
periodic `getUpdates` polling in `run` mode (default every 30s) with `/status` support.
- VK backoff on rate limit/network: `5s, 15s, 60s, 300s`
- Recovery system message:
if previous run ended uncleanly, next startup sends a Telegram system alert.

## Quick setup (under 10 minutes)

1. Install Python 3.9+ on Raspberry Pi.
2. Clone/copy project.
3. Create and activate virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
# Windows PowerShell:
# .venv\Scripts\Activate.ps1
```

4. Install runtime dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

5. Create `.env` in project root:

```dotenv
VK_ACCESS_TOKEN=your_vk_token
# Use ONE of these:
VK_DOMAIN=some_public_group
# VK_OWNER_ID=-123456

TG_BOT_TOKEN=123456:ABCDEF
TG_CHAT_ID=123456789

KEYWORDS=discount,sale,promo
# KEYWORDS_REGEX=promo\s+\d+

MATCH_MODE=any
POLL_INTERVAL_SECONDS=30
TG_UPDATES_INTERVAL_SECONDS=30
VK_COUNT=10
VK_FILTER=owner
VK_API_VERSION=5.199
TIMEZONE=Europe/Berlin
STATE_PATH=state/vk_wall_monitor.sqlite
CATCH_UP=1
```

## Telegram setup

1. Open `@BotFather` in Telegram.
2. Run `/newbot` and save the bot token as `TG_BOT_TOKEN`.
3. Send any message to your bot from target chat.
4. Get chat id:
`https://api.telegram.org/bot<TG_BOT_TOKEN>/getUpdates` and use `chat.id` from response as `TG_CHAT_ID`.

## VK token note

Provide `VK_ACCESS_TOKEN` manually (user token or service token).
OAuth automation is intentionally not included.

## CLI usage

Run loop:

```bash
python -m vk_wall_monitor run
```

Check once (for cron/timer):

```bash
python -m vk_wall_monitor check-once
```

Status command (in configured `TG_CHAT_ID`):

```text
/status
```

Bot replies with:
- `🟢 Running: yes`
- `🕒 Last check: ...`
- `🔗 Last checked post: https://vk.com/wall{owner_id}_{post_id}` (or `n/a`)

`check-once` is read-only: it does not update SQLite state.
Use it for diagnostics/manual checks, not for long-running deduplicated monitoring.
If no state exists yet, it scans the fetched batch immediately (does not baseline-and-exit).

VK connectivity test:

```bash
python -m vk_wall_monitor test-vk
# also accepted:
# python -m vk_wall_monitor test_vk
```

Telegram test:

```bash
python -m vk_wall_monitor test-telegram
```

Common options:

- `--interval-seconds 30`
- `--count 10`
- `--mode any|all`
- `--dry-run`
- `--state-path ./state/vk_wall_monitor.sqlite`
- `--catch-up` / `--no-catch-up`

Optional: install as a CLI script:

```bash
python -m pip install -e .
vk-wall-monitor run
```

## systemd (recommended for long run)

Example `/etc/systemd/system/vk-wall-monitor.service`:

```ini
[Unit]
Description=VK Wall Monitor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/VKMonitor
ExecStart=/home/pi/VKMonitor/.venv/bin/python -m vk_wall_monitor run
Restart=always
RestartSec=5
EnvironmentFile=/home/pi/VKMonitor/.env

[Install]
WantedBy=multi-user.target
```

Enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now vk-wall-monitor.service
sudo systemctl status vk-wall-monitor.service
```

## cron option (single checks, read-only)

Every minute:

```cron
* * * * * cd /home/pi/VKMonitor && /home/pi/VKMonitor/.venv/bin/python -m vk_wall_monitor check-once >> /var/log/vk-monitor.log 2>&1
```

Note: because `check-once` is read-only, repeated cron runs can re-send alerts for the same post.
Use `run` with systemd if you need deduplication across polling cycles.

## Operational behavior

- First run is baseline-only: no historical spam.
- `run` updates state and prevents duplicate notifications across polls.
- `check-once` does not save state (read-only mode) and may re-notify the same post on repeated runs.
- On unclean stop (crash/power loss), next startup sends one recovery system message.
- Tokens are never logged.

## Tests

```bash
python -m pip install pytest responses
pytest -q
```
