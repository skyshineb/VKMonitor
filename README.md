# VK Wall Monitor

Small production-ready monitor for one public VK group wall.

It polls VK `wall.get`, checks keywords (or regex), and sends instant Telegram messages for matching new posts.
State is stored in SQLite to avoid duplicate alerts after restart.

## Features

- Default polling interval: 30 seconds (safe for one group).
- VK target by either `VK_DOMAIN` or `VK_OWNER_ID`.
- Keyword modes: `any` (default) / `all`.
- Optional regex mode (`KEYWORDS_REGEX`) with case-insensitive search.
- Searches in post text and `copy_history` text.
- Duplicate protection with SQLite:
  - `last_seen_post_id` per wall owner
  - `notified_posts` table to avoid re-sending same post
- Telegram safety:
  - max 1 msg/sec local pacing
  - retries once on Telegram 429 using `retry_after`
- VK backoff on rate limit/network:
  - 5s, 15s, 60s, 300s
- Recovery system message:
  - If previous run ended uncleanly, next startup sends a Telegram system alert.

## Quick setup (under 10 minutes)

1. Install Python 3.11+ on Raspberry Pi.
2. Clone/copy project.
3. Install dependencies:

```bash
python -m pip install -e .
python -m pip install -e ".[test]"   # optional, for tests
```

4. Create `.env` in project root:

```dotenv
VK_ACCESS_TOKEN=your_vk_token
# Use ONE of these:
VK_DOMAIN=some_public_group
# VK_OWNER_ID=-123456

TG_BOT_TOKEN=123456:ABCDEF
TG_CHAT_ID=123456789

KEYWORDS=discount, акция, promo
# KEYWORDS_REGEX=promo\s+\d+

MATCH_MODE=any
POLL_INTERVAL_SECONDS=30
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
   - `https://api.telegram.org/bot<TG_BOT_TOKEN>/getUpdates`
   - Find `chat.id` in response and use as `TG_CHAT_ID`.

## VK token note

Provide `VK_ACCESS_TOKEN` manually (user token or service token).
OAuth automation is intentionally not included.

## CLI usage

Run loop:

```bash
python -m vk_wall_monitor run
# or
vk-wall-monitor run
```

Check once (for cron/timer):

```bash
vk-wall-monitor check-once
```

VK connectivity test:

```bash
vk-wall-monitor test-vk
```

Telegram test:

```bash
vk-wall-monitor test-telegram
```

Common options (all commands):

- `--interval-seconds 30`
- `--count 10`
- `--mode any|all`
- `--dry-run`
- `--state-path ./state/vk_wall_monitor.sqlite`
- `--catch-up` / `--no-catch-up`

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
ExecStart=/usr/bin/python -m vk_wall_monitor run
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

## cron option (single checks)

Every minute:

```cron
* * * * * cd /home/pi/VKMonitor && /usr/bin/python -m vk_wall_monitor check-once >> /var/log/vk-monitor.log 2>&1
```

## Operational behavior

- First run is baseline-only: no historical spam.
- `check-once` twice with same data does not send duplicates.
- On unclean stop (crash/power loss), next startup sends one recovery system message.
- Tokens are never logged.

## Tests

```bash
pytest -q
```
