from __future__ import annotations

import argparse
import logging
import os
import re
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


VK_API_URL = "https://api.vk.com/method/wall.get"
TG_API_BASE = "https://api.telegram.org"
BACKOFF_SCHEDULE_SECONDS = (5, 15, 60, 300)


class VKMonitorError(Exception):
    """Base exception for monitor errors."""


class VKAuthError(VKMonitorError):
    """Raised for VK auth issues (error code 5)."""


class VKTransientError(VKMonitorError):
    """Raised for transient VK/network errors."""


class VKFatalError(VKMonitorError):
    """Raised for non-transient VK errors."""


class TelegramError(VKMonitorError):
    """Raised when Telegram cannot accept a message."""


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def parse_keywords(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [normalize_whitespace(item) for item in raw.split(",") if normalize_whitespace(item)]


def parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Config:
    vk_access_token: str
    tg_bot_token: str | None
    tg_chat_id: str | None
    vk_domain: str | None
    vk_owner_id: int | None
    interval_seconds: int
    count: int
    wall_filter: str
    vk_api_version: str
    keywords: list[str]
    keywords_regex: str | None
    mode: str
    dry_run: bool
    catch_up: bool
    state_path: Path
    timezone_name: str
    http_timeout_seconds: float = 15.0

    def validate(self, command: str) -> None:
        if not self.vk_access_token and command in {"run", "check-once", "check_once", "test-vk", "test_vk"}:
            raise ValueError("VK_ACCESS_TOKEN is required.")
        if not self.vk_domain and self.vk_owner_id is None:
            raise ValueError("Set either VK_DOMAIN or VK_OWNER_ID.")
        if self.vk_domain and self.vk_owner_id is not None:
            raise ValueError("Set only one of VK_DOMAIN or VK_OWNER_ID.")
        if self.count < 1 or self.count > 100:
            raise ValueError("count must be in range 1..100.")
        if self.interval_seconds < 1:
            raise ValueError("interval_seconds must be >= 1.")
        if self.mode not in {"any", "all"}:
            raise ValueError("mode must be 'any' or 'all'.")
        if command in {"test-telegram", "test_telegram"} and (not self.tg_bot_token or not self.tg_chat_id):
            raise ValueError("TG_BOT_TOKEN and TG_CHAT_ID are required for test-telegram.")
        if command in {"run", "check-once", "check_once"} and not self.dry_run:
            if not self.tg_bot_token or not self.tg_chat_id:
                raise ValueError("TG_BOT_TOKEN and TG_CHAT_ID are required unless --dry-run is set.")
        try:
            ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Invalid timezone: {self.timezone_name}") from exc


class StateStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS wall_state (
                owner_id INTEGER PRIMARY KEY,
                last_seen_post_id INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS notified_posts (
                owner_id INTEGER NOT NULL,
                post_id INTEGER NOT NULL,
                notified_at TEXT NOT NULL,
                PRIMARY KEY (owner_id, post_id)
            );

            CREATE TABLE IF NOT EXISTS runtime_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def get_last_seen(self, owner_id: int) -> int | None:
        row = self.conn.execute(
            "SELECT last_seen_post_id FROM wall_state WHERE owner_id = ?",
            (owner_id,),
        ).fetchone()
        return int(row["last_seen_post_id"]) if row else None

    def set_last_seen(self, owner_id: int, post_id: int) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO wall_state(owner_id, last_seen_post_id, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(owner_id) DO UPDATE SET
                last_seen_post_id = excluded.last_seen_post_id,
                updated_at = excluded.updated_at
            """,
            (owner_id, post_id, now_iso),
        )
        self.conn.commit()

    def is_notified(self, owner_id: int, post_id: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM notified_posts WHERE owner_id = ? AND post_id = ?",
            (owner_id, post_id),
        ).fetchone()
        return row is not None

    def mark_notified(self, owner_id: int, post_id: int) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT OR IGNORE INTO notified_posts(owner_id, post_id, notified_at)
            VALUES (?, ?, ?)
            """,
            (owner_id, post_id, now_iso),
        )
        self.conn.commit()

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.conn.execute("SELECT value FROM runtime_meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            """
            INSERT INTO runtime_meta(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        self.conn.commit()

    def set_last_error(self, message: str) -> None:
        self.set_meta("last_error", message[:2000])

    def start_runtime(self) -> tuple[bool, str | None]:
        initialized = parse_bool(self.get_meta("initialized"), False)
        shutdown_clean = parse_bool(self.get_meta("shutdown_clean"), True)
        last_error = self.get_meta("last_error")
        was_unclean = initialized and not shutdown_clean

        self.set_meta("initialized", "1")
        self.set_meta("shutdown_clean", "0")
        self.set_meta("last_start_at", datetime.now(timezone.utc).isoformat())
        return was_unclean, last_error

    def mark_clean_shutdown(self) -> None:
        self.set_meta("shutdown_clean", "1")
        self.set_meta("last_stop_at", datetime.now(timezone.utc).isoformat())


class Monitor:
    def __init__(
        self,
        config: Config,
        state: StateStore | None = None,
        session: requests.Session | None = None,
        sleeper=time.sleep,
        monotonic=time.monotonic,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config
        self.state = state or StateStore(config.state_path)
        self.session = session or requests.Session()
        self.sleep = sleeper
        self.monotonic = monotonic
        self.logger = logger or logging.getLogger("vk_wall_monitor")
        self.last_telegram_send_monotonic: float | None = None
        self.regex = (
            re.compile(self.config.keywords_regex, re.IGNORECASE)
            if self.config.keywords_regex
            else None
        )

    def close(self) -> None:
        self.state.close()
        self.session.close()

    def _vk_params(self) -> dict[str, Any]:
        params: dict[str, Any] = {
            "access_token": self.config.vk_access_token,
            "count": self.config.count,
            "filter": self.config.wall_filter,
            "v": self.config.vk_api_version,
        }
        if self.config.vk_domain:
            params["domain"] = self.config.vk_domain
        else:
            params["owner_id"] = self.config.vk_owner_id
        return params

    def fetch_vk_posts(self) -> list[dict[str, Any]]:
        try:
            response = self.session.get(
                VK_API_URL,
                params=self._vk_params(),
                timeout=self.config.http_timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            raise VKTransientError(f"VK request failed: {exc}") from exc
        except requests.RequestException as exc:
            raise VKFatalError(f"VK request error: {exc}") from exc
        except ValueError as exc:
            raise VKFatalError(f"VK response is not valid JSON: {exc}") from exc

        if "error" in payload:
            error = payload["error"] or {}
            code = int(error.get("error_code", 0))
            message = str(error.get("error_msg", "VK API error"))
            if code == 5:
                raise VKAuthError(f"VK auth error (code 5): {message}")
            if code == 6:
                raise VKTransientError(f"VK rate limited (code 6): {message}")
            raise VKFatalError(f"VK API error code {code}: {message}")

        items = payload.get("response", {}).get("items", [])
        if not isinstance(items, list):
            raise VKFatalError("VK payload does not contain response.items list.")
        return items

    def _post_texts(self, post: dict[str, Any]) -> list[str]:
        texts = [str(post.get("text", ""))]
        copy_history = post.get("copy_history")
        if isinstance(copy_history, list):
            for entry in copy_history:
                if isinstance(entry, dict):
                    texts.append(str(entry.get("text", "")))
        return texts

    def match_post(self, post: dict[str, Any]) -> list[str]:
        texts = [normalize_whitespace(text).lower() for text in self._post_texts(post)]
        if self.regex:
            found: list[str] = []
            for text in texts:
                found.extend([m.group(0) for m in self.regex.finditer(text)])
            unique = list(dict.fromkeys([item for item in found if item]))
            return unique if unique else []

        normalized_keywords = [keyword.lower() for keyword in self.config.keywords]
        if not normalized_keywords:
            return []

        present_keywords = [
            keyword for keyword in normalized_keywords if any(keyword in text for text in texts)
        ]
        if self.config.mode == "all":
            if len(present_keywords) == len(normalized_keywords):
                return normalized_keywords
            return []
        return present_keywords

    def _format_timestamp(self, unix_ts: int) -> str:
        local_tz = ZoneInfo(self.config.timezone_name)
        dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc).astimezone(local_tz)
        return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

    def build_post_message(self, post: dict[str, Any], matched_terms: list[str]) -> str:
        owner_id = int(post["owner_id"])
        post_id = int(post["id"])
        snippet = normalize_whitespace(str(post.get("text", "")))[:300]
        keyword_display = ", ".join(matched_terms) if matched_terms else "n/a"
        group_identifier = self.config.vk_domain or str(owner_id)
        link = f"https://vk.com/wall{owner_id}_{post_id}"
        timestamp = self._format_timestamp(int(post["date"]))
        return "\n".join(
            [
                "VK: New post matched keywords",
                f"Group: {group_identifier}",
                f"Post time: {timestamp}",
                f"Matched: {keyword_display}",
                f"Text: {snippet}",
                f"Link: {link}",
            ]
        )

    def send_telegram_message(self, text: str) -> None:
        if self.config.dry_run:
            self.logger.info("[DRY RUN] Telegram message:\n%s", text)
            return
        if not self.config.tg_bot_token or not self.config.tg_chat_id:
            raise TelegramError("Telegram token/chat is not configured.")

        if self.last_telegram_send_monotonic is not None:
            elapsed = self.monotonic() - self.last_telegram_send_monotonic
            if elapsed < 1.0:
                self.sleep(1.0 - elapsed)

        url = f"{TG_API_BASE}/bot{self.config.tg_bot_token}/sendMessage"
        data = {"chat_id": self.config.tg_chat_id, "text": text}
        self._send_telegram_with_retry(url, data)
        self.last_telegram_send_monotonic = self.monotonic()

    def _send_telegram_with_retry(self, url: str, data: dict[str, str]) -> None:
        response = self.session.post(url, data=data, timeout=self.config.http_timeout_seconds)
        if response.status_code == 429:
            retry_after = 1
            try:
                payload = response.json()
                retry_after = int(
                    (payload.get("parameters", {}) or {}).get("retry_after", retry_after)
                )
            except ValueError:
                pass
            self.logger.warning("Telegram rate limited. Retrying in %ss.", retry_after)
            self.sleep(max(1, retry_after))
            response = self.session.post(url, data=data, timeout=self.config.http_timeout_seconds)

        if response.status_code >= 400:
            raise TelegramError(f"Telegram HTTP {response.status_code}: {response.text[:300]}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise TelegramError(f"Telegram invalid JSON response: {exc}") from exc
        if not payload.get("ok", False):
            raise TelegramError(f"Telegram API error: {payload}")

    def maybe_send_recovery_message(self) -> bool:
        was_unclean, last_error = self.state.start_runtime()
        if not was_unclean:
            return False

        message = "VK monitor recovered after unclean stop."
        if last_error:
            message += f"\nLast error: {last_error[:500]}"
        self.send_telegram_message(message)
        return True

    def process_once(self) -> int:
        posts = self.fetch_vk_posts()
        if not posts:
            self.logger.info("VK returned no posts.")
            return 0

        owner_id = int(posts[0].get("owner_id", self.config.vk_owner_id))
        last_seen = self.state.get_last_seen(owner_id)
        post_ids = [int(post.get("id", 0)) for post in posts if "id" in post]
        if not post_ids:
            self.logger.info("No valid post IDs in VK response.")
            return 0

        if last_seen is None:
            baseline = max(post_ids)
            self.logger.info("State not initialized for owner %s, baseline=%s.", owner_id, baseline)
            if not self.config.dry_run:
                self.state.set_last_seen(owner_id, baseline)
            return 0

        new_posts = [post for post in posts if int(post["id"]) > last_seen]
        if not self.config.catch_up and new_posts:
            newest = max(new_posts, key=lambda post: int(post["id"]))
            new_posts = [newest]
        new_posts.sort(key=lambda post: int(post["id"]))

        sent_count = 0
        for post in new_posts:
            post_id = int(post["id"])
            if self.state.is_notified(owner_id, post_id):
                self.logger.info("Post %s already notified; skipping duplicate.", post_id)
                if not self.config.dry_run:
                    self.state.set_last_seen(owner_id, post_id)
                continue

            matched_terms = self.match_post(post)
            if matched_terms:
                message = self.build_post_message(post, matched_terms)
                self.send_telegram_message(message)
                sent_count += 1
                if not self.config.dry_run:
                    self.state.mark_notified(owner_id, post_id)
                    self.state.set_last_seen(owner_id, post_id)
            else:
                if not self.config.dry_run:
                    self.state.set_last_seen(owner_id, post_id)

        return sent_count

    def check_once_with_backoff(self, max_attempts: int = 5) -> int:
        attempt = 0
        while True:
            try:
                return self.process_once()
            except VKTransientError as exc:
                if attempt >= max_attempts - 1:
                    raise
                delay = BACKOFF_SCHEDULE_SECONDS[min(attempt, len(BACKOFF_SCHEDULE_SECONDS) - 1)]
                self.logger.warning("Transient VK error: %s. Retrying in %ss.", exc, delay)
                self.sleep(delay)
                attempt += 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor VK wall posts and notify Telegram.")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))

    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_flags(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--interval-seconds", type=int, default=None)
        subparser.add_argument("--count", type=int, default=None)
        subparser.add_argument("--mode", choices=["any", "all"], default=None)
        subparser.add_argument("--dry-run", action="store_true")
        subparser.add_argument("--state-path", default=None)
        subparser.add_argument("--catch-up", dest="catch_up", action="store_true")
        subparser.add_argument("--no-catch-up", dest="catch_up", action="store_false")
        subparser.set_defaults(catch_up=None)

    add_common_flags(subparsers.add_parser("run", help="Run continuous polling loop."))
    add_common_flags(
        subparsers.add_parser("check-once", aliases=["check_once"], help="Poll once and exit.")
    )
    add_common_flags(
        subparsers.add_parser("test-vk", aliases=["test_vk"], help="Call VK and print latest posts.")
    )
    add_common_flags(
        subparsers.add_parser(
            "test-telegram",
            aliases=["test_telegram"],
            help="Send Telegram test message.",
        )
    )
    return parser


def build_config(args: argparse.Namespace) -> Config:
    if load_dotenv is not None:
        load_dotenv()

    def env(name: str, fallback: str | None = None) -> str | None:
        value = os.getenv(name)
        return value if value is not None else fallback

    owner_id_raw = env("VK_OWNER_ID")
    owner_id = int(owner_id_raw) if owner_id_raw else None

    count = args.count if args.count is not None else int(env("VK_COUNT", "10"))
    interval = (
        args.interval_seconds
        if args.interval_seconds is not None
        else int(env("POLL_INTERVAL_SECONDS", "30"))
    )
    mode = (args.mode or env("MATCH_MODE", "any") or "any").strip().lower()
    catch_up = args.catch_up
    if catch_up is None:
        catch_up = parse_bool(env("CATCH_UP"), True)
    state_path_raw = args.state_path or env("STATE_PATH", "state/vk_wall_monitor.sqlite")

    return Config(
        vk_access_token=env("VK_ACCESS_TOKEN", "") or "",
        tg_bot_token=env("TG_BOT_TOKEN"),
        tg_chat_id=env("TG_CHAT_ID"),
        vk_domain=env("VK_DOMAIN"),
        vk_owner_id=owner_id,
        interval_seconds=interval,
        count=count,
        wall_filter=env("VK_FILTER", "owner") or "owner",
        vk_api_version=env("VK_API_VERSION", "5.199") or "5.199",
        keywords=parse_keywords(env("KEYWORDS")),
        keywords_regex=env("KEYWORDS_REGEX"),
        mode=mode,
        dry_run=bool(args.dry_run),
        catch_up=bool(catch_up),
        state_path=Path(state_path_raw),
        timezone_name=env("TIMEZONE", "Europe/Berlin") or "Europe/Berlin",
    )


def _install_sigterm_handler() -> None:
    def _handle_sigterm(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_sigterm)


def command_test_vk(monitor: Monitor) -> int:
    posts = monitor.fetch_vk_posts()
    print(f"Received {len(posts)} posts from VK.")
    for post in posts[:5]:
        snippet = normalize_whitespace(str(post.get("text", "")))[:80]
        print(f"id={post.get('id')} owner_id={post.get('owner_id')} text={snippet}")
    return 0


def command_test_telegram(monitor: Monitor) -> int:
    now = datetime.now(timezone.utc).isoformat()
    monitor.send_telegram_message(f"VK monitor test message at {now}")
    print("Telegram test message sent.")
    return 0


def command_check_once(monitor: Monitor) -> int:
    sent = monitor.check_once_with_backoff()
    print(f"check-once complete. sent={sent}")
    return 0


def command_run(monitor: Monitor) -> int:
    _install_sigterm_handler()
    monitor.maybe_send_recovery_message()
    logger = logging.getLogger("vk_wall_monitor")

    backoff_index = 0
    try:
        while True:
            monitor.process_once()
            backoff_index = 0
            monitor.sleep(monitor.config.interval_seconds)
    except KeyboardInterrupt:
        logger.info("Received stop signal, shutting down cleanly.")
        monitor.state.mark_clean_shutdown()
        return 0
    except VKTransientError as exc:
        delay = BACKOFF_SCHEDULE_SECONDS[min(backoff_index, len(BACKOFF_SCHEDULE_SECONDS) - 1)]
        monitor.state.set_last_error(str(exc))
        logger.warning("Transient VK error in run loop: %s. Sleeping %ss.", exc, delay)
        monitor.sleep(delay)
        return command_run(monitor)
    except Exception as exc:
        monitor.state.set_last_error(str(exc))
        logger.exception("Monitor stopped with error.")
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        config = build_config(args)
        config.validate(args.command)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    monitor = Monitor(config=config)
    try:
        if args.command == "run":
            return command_run(monitor)
        if args.command in {"check-once", "check_once"}:
            return command_check_once(monitor)
        if args.command in {"test-vk", "test_vk"}:
            return command_test_vk(monitor)
        if args.command in {"test-telegram", "test_telegram"}:
            return command_test_telegram(monitor)
        parser.error(f"Unknown command: {args.command}")
        return 2
    except VKAuthError as exc:
        monitor.state.set_last_error(str(exc))
        print(str(exc), file=sys.stderr)
        return 1
    except VKMonitorError as exc:
        monitor.state.set_last_error(str(exc))
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        monitor.close()


if __name__ == "__main__":
    raise SystemExit(main())
