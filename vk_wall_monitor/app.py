from __future__ import annotations

import argparse
import io
import json
import logging
import mimetypes
import os
import re
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


VK_API_URL = "https://api.vk.com/method/wall.get"
TG_API_BASE = "https://api.telegram.org"
BACKOFF_SCHEDULE_SECONDS = (5, 15, 60, 300)
TELEGRAM_TEXT_LIMIT = 4096
TELEGRAM_CAPTION_LIMIT = 1024
TELEGRAM_MEDIA_GROUP_LIMIT = 10
DAILY_DIGEST_RETRY_SECONDS = 300


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


def parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def parse_keywords(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [normalize_whitespace(item) for item in raw.split(",") if normalize_whitespace(item)]


def parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_time_of_day(raw: str) -> dt_time:
    try:
        return datetime.strptime(raw.strip(), "%H:%M").time()
    except ValueError as exc:
        raise ValueError("daily_digest_time must be in HH:MM format.") from exc


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
    tg_updates_interval_seconds: int
    enable_instant_alerts: bool
    enable_daily_digest: bool
    daily_digest_time: str
    digest_line_excludes: list[str]
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
        if self.tg_updates_interval_seconds < 1:
            raise ValueError("tg_updates_interval_seconds must be >= 1.")
        if self.mode not in {"any", "all"}:
            raise ValueError("mode must be 'any' or 'all'.")
        if command == "run" and not self.enable_instant_alerts and not self.enable_daily_digest:
            raise ValueError("At least one of ENABLE_INSTANT_ALERTS or ENABLE_DAILY_DIGEST must be enabled.")
        if command in {"test-telegram", "test_telegram"} and (not self.tg_bot_token or not self.tg_chat_id):
            raise ValueError("TG_BOT_TOKEN and TG_CHAT_ID are required for test-telegram.")
        if command in {"run", "check-once", "check_once"} and not self.dry_run:
            if not self.tg_bot_token or not self.tg_chat_id:
                raise ValueError("TG_BOT_TOKEN and TG_CHAT_ID are required unless --dry-run is set.")
        try:
            ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Invalid timezone: {self.timezone_name}") from exc
        parse_time_of_day(self.daily_digest_time)


class StateStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._meta_cache: dict[str, str] = {}
        self._pending_meta: dict[str, str] = {}
        self._meta_cache_loaded = False
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
        self.commit_with_pending_meta()
        self.conn.close()

    def _load_meta_cache(self) -> None:
        if self._meta_cache_loaded:
            return
        rows = self.conn.execute("SELECT key, value FROM runtime_meta").fetchall()
        self._meta_cache = {str(row["key"]): str(row["value"]) for row in rows}
        self._meta_cache_loaded = True

    def _upsert_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            """
            INSERT INTO runtime_meta(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )

    def flush_pending_meta(self) -> None:
        if not self._pending_meta:
            return
        pending_items = list(self._pending_meta.items())
        self._pending_meta.clear()
        for key, value in pending_items:
            self._upsert_meta(key, value)

    def commit(self) -> None:
        self.conn.commit()

    def commit_with_pending_meta(self) -> None:
        self.flush_pending_meta()
        self.conn.commit()

    def get_last_seen(self, owner_id: int) -> int | None:
        row = self.conn.execute(
            "SELECT last_seen_post_id FROM wall_state WHERE owner_id = ?",
            (owner_id,),
        ).fetchone()
        return int(row["last_seen_post_id"]) if row else None

    def set_last_seen(self, owner_id: int, post_id: int, commit: bool = True) -> None:
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
        if commit:
            self.commit_with_pending_meta()

    def is_notified(self, owner_id: int, post_id: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM notified_posts WHERE owner_id = ? AND post_id = ?",
            (owner_id, post_id),
        ).fetchone()
        return row is not None

    def mark_notified(self, owner_id: int, post_id: int, commit: bool = True) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT OR IGNORE INTO notified_posts(owner_id, post_id, notified_at)
            VALUES (?, ?, ?)
            """,
            (owner_id, post_id, now_iso),
        )
        if commit:
            self.commit_with_pending_meta()

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        self._load_meta_cache()
        value = self._meta_cache.get(key)
        return value if value is not None else default

    def set_meta(self, key: str, value: str, immediate: bool = False) -> None:
        self._load_meta_cache()
        current = self._pending_meta.get(key, self._meta_cache.get(key))
        if current == value:
            return

        self._meta_cache[key] = value
        if immediate:
            self._pending_meta.pop(key, None)
            self._upsert_meta(key, value)
            return
        self._pending_meta[key] = value

    def set_last_error(self, message: str) -> None:
        self.set_meta("last_error", message[:2000], immediate=False)

    def start_runtime(self) -> tuple[bool, str | None]:
        initialized = parse_bool(self.get_meta("initialized"), False)
        shutdown_clean = parse_bool(self.get_meta("shutdown_clean"), True)
        last_error = self.get_meta("last_error")
        was_unclean = initialized and not shutdown_clean

        self.set_meta("initialized", "1", immediate=False)
        self.set_meta("shutdown_clean", "0", immediate=False)
        self.set_meta("last_start_at", datetime.now(timezone.utc).isoformat(), immediate=False)
        return was_unclean, last_error

    def mark_clean_shutdown(self) -> None:
        self.set_meta("shutdown_clean", "1", immediate=False)
        self.set_meta("last_stop_at", datetime.now(timezone.utc).isoformat(), immediate=False)
        self.commit_with_pending_meta()


@dataclass
class CheckResult:
    sent_count: int
    last_checked_owner_id: int | None
    last_checked_post_id: int | None


@dataclass
class DailyDigestPost:
    owner_id: int
    post_id: int
    unix_ts: int
    text: str
    photo_urls: list[str]
    digest_date_local: str


@dataclass
class DownloadedPhoto:
    source_url: str
    filename: str
    content_type: str
    content: bytes


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
        self.daily_digest_time_of_day = parse_time_of_day(self.config.daily_digest_time)
        self.daily_digest_buffer: dict[str, dict[tuple[int, int], DailyDigestPost]] = {}
        self.next_daily_digest_at: datetime | None = None
        self.pending_daily_digest_date: date | None = None

    def close(self) -> None:
        self.state.close()
        self.session.close()

    def _local_tz(self) -> ZoneInfo:
        return ZoneInfo(self.config.timezone_name)

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def _now_local(self) -> datetime:
        return self._now_utc().astimezone(self._local_tz())

    def _vk_params(self, offset: int = 0, count: int | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {
            "access_token": self.config.vk_access_token,
            "count": count if count is not None else self.config.count,
            "filter": self.config.wall_filter,
            "offset": offset,
            "v": self.config.vk_api_version,
        }
        if self.config.vk_domain:
            params["domain"] = self.config.vk_domain
        else:
            params["owner_id"] = self.config.vk_owner_id
        return params

    def fetch_vk_posts(self, offset: int = 0, count: int | None = None) -> list[dict[str, Any]]:
        try:
            response = self.session.get(
                VK_API_URL,
                params=self._vk_params(offset=offset, count=count),
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

    def fetch_posts_for_local_date(self, target_date: date) -> list[dict[str, Any]]:
        page_size = 100
        offset = 0
        matched: list[dict[str, Any]] = []
        seen_post_ids: set[int] = set()

        while True:
            items = self.fetch_vk_posts(offset=offset, count=page_size)
            if not items:
                break

            has_at_or_after_target = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                try:
                    post_id = int(item.get("id", 0))
                    unix_ts = int(item["date"])
                except (KeyError, TypeError, ValueError):
                    continue
                local_date = self._local_datetime_from_unix(unix_ts).date()
                if local_date >= target_date:
                    has_at_or_after_target = True
                if local_date == target_date and post_id not in seen_post_ids:
                    seen_post_ids.add(post_id)
                    matched.append(item)

            if len(items) < page_size or not has_at_or_after_target:
                break
            offset += len(items)

        matched.sort(key=lambda post: (int(post.get("date", 0)), int(post.get("id", 0))))
        return matched

    def _post_texts(self, post: dict[str, Any]) -> list[str]:
        texts = [str(post.get("text", ""))]
        copy_history = post.get("copy_history")
        if isinstance(copy_history, list):
            for entry in copy_history:
                if isinstance(entry, dict):
                    texts.append(str(entry.get("text", "")))
        return texts

    def _extract_best_photo_url(self, photo: dict[str, Any]) -> str | None:
        sizes = photo.get("sizes")
        if not isinstance(sizes, list):
            return None
        best_url: str | None = None
        best_area = -1
        for size in sizes:
            if not isinstance(size, dict):
                continue
            url = size.get("url")
            if not isinstance(url, str) or not url:
                continue
            width = int(size.get("width", 0) or 0)
            height = int(size.get("height", 0) or 0)
            area = width * height
            if area > best_area:
                best_area = area
                best_url = url
        return best_url

    def _collect_photo_urls(self, post: dict[str, Any]) -> list[str]:
        photo_urls: list[str] = []
        seen: set[str] = set()

        def consume_attachments(attachments: Any) -> None:
            if not isinstance(attachments, list):
                return
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    continue
                if attachment.get("type") != "photo":
                    continue
                photo = attachment.get("photo")
                if not isinstance(photo, dict):
                    continue
                url = self._extract_best_photo_url(photo)
                if url and url not in seen:
                    seen.add(url)
                    photo_urls.append(url)

        consume_attachments(post.get("attachments"))
        copy_history = post.get("copy_history")
        if isinstance(copy_history, list):
            for entry in copy_history:
                if isinstance(entry, dict):
                    consume_attachments(entry.get("attachments"))
        return photo_urls

    def _filter_digest_text_lines(self, texts: list[str]) -> str:
        excludes = [item.casefold() for item in self.config.digest_line_excludes]
        kept_lines: list[str] = []
        for raw_text in texts:
            for line in raw_text.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                folded = stripped.casefold()
                if any(exclude in folded for exclude in excludes):
                    continue
                kept_lines.append(stripped)
        if not kept_lines:
            return "[без текста]"
        return "\n".join(kept_lines)

    def build_daily_digest_post(self, post: dict[str, Any]) -> DailyDigestPost | None:
        try:
            owner_id = int(post["owner_id"])
            post_id = int(post["id"])
            unix_ts = int(post["date"])
        except (KeyError, TypeError, ValueError):
            return None

        digest_dt = self._local_datetime_from_unix(unix_ts)
        return DailyDigestPost(
            owner_id=owner_id,
            post_id=post_id,
            unix_ts=unix_ts,
            text=self._filter_digest_text_lines(self._post_texts(post)),
            photo_urls=self._collect_photo_urls(post),
            digest_date_local=digest_dt.date().isoformat(),
        )

    def _buffer_daily_digest_post(self, post: dict[str, Any]) -> None:
        digest_post = self.build_daily_digest_post(post)
        if digest_post is None:
            return
        entries = self.daily_digest_buffer.setdefault(digest_post.digest_date_local, {})
        entries[(digest_post.owner_id, digest_post.post_id)] = digest_post

    def _clear_daily_digest_buffer_up_to(self, target_date: date) -> None:
        for digest_date in list(self.daily_digest_buffer):
            if digest_date <= target_date.isoformat():
                self.daily_digest_buffer.pop(digest_date, None)

    def match_post(self, post: dict[str, Any]) -> list[str]:
        texts = [normalize_whitespace(text) for text in self._post_texts(post)]
        if self.regex:
            found: list[str] = []
            for text in texts:
                found.extend([m.group(0) for m in self.regex.finditer(text)])
            seen: set[str] = set()
            unique: list[str] = []
            for item in found:
                normalized = item.casefold()
                if item and normalized not in seen:
                    seen.add(normalized)
                    unique.append(item)
            return unique if unique else []

        keyword_pairs = [(keyword, keyword.casefold()) for keyword in self.config.keywords]
        if not keyword_pairs:
            return []
        text_casefolded = [text.casefold() for text in texts]

        present_keywords = [
            original_keyword
            for original_keyword, folded_keyword in keyword_pairs
            if any(folded_keyword in text for text in text_casefolded)
        ]
        if self.config.mode == "all":
            if len(present_keywords) == len(keyword_pairs):
                return [original_keyword for original_keyword, _ in keyword_pairs]
            return []
        return present_keywords

    def _local_datetime_from_unix(self, unix_ts: int) -> datetime:
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).astimezone(self._local_tz())

    def _format_timestamp(self, unix_ts: int) -> str:
        return self._local_datetime_from_unix(unix_ts).strftime("%Y-%m-%d %H:%M:%S %Z")

    def _format_iso_timestamp_for_status(self, iso_ts: str | None) -> str:
        if not iso_ts:
            return "n/a"
        try:
            dt = datetime.fromisoformat(iso_ts)
        except ValueError:
            return "n/a"
        return dt.astimezone(self._local_tz()).strftime("%Y-%m-%d %H:%M:%S %Z")

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

    def _send_telegram_request(
        self,
        method: str,
        data: dict[str, str],
        files: dict[str, tuple[str, io.BytesIO, str]] | None = None,
        error_context: str | None = None,
    ) -> None:
        url = f"{TG_API_BASE}/bot{self.config.tg_bot_token}/{method}"
        response = self.session.post(
            url,
            data=data,
            files=files,
            timeout=self.config.http_timeout_seconds,
        )
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
            response = self.session.post(
                url,
                data=data,
                files=files,
                timeout=self.config.http_timeout_seconds,
            )

        if response.status_code >= 400:
            if error_context:
                self.logger.warning(
                    "Telegram %s failed (%s): %s",
                    method,
                    error_context,
                    response.text[:300],
                )
            raise TelegramError(f"Telegram HTTP {response.status_code}: {response.text[:300]}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise TelegramError(f"Telegram invalid JSON response: {exc}") from exc
        if not payload.get("ok", False):
            if error_context:
                self.logger.warning("Telegram %s API error (%s): %s", method, error_context, payload)
            raise TelegramError(f"Telegram API error: {payload}")

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

        self._send_telegram_request(
            "sendMessage",
            {"chat_id": self.config.tg_chat_id, "text": text},
        )
        self.last_telegram_send_monotonic = self.monotonic()

    def _guess_photo_filename(self, url: str, content_type: str | None) -> str:
        parsed = urlparse(url)
        filename = Path(parsed.path).name or "photo"
        if "." not in filename:
            guessed_ext = mimetypes.guess_extension((content_type or "").split(";")[0].strip())
            if guessed_ext:
                filename = f"{filename}{guessed_ext}"
            else:
                filename = f"{filename}.jpg"
        return filename

    def _download_photo(self, photo_url: str) -> DownloadedPhoto:
        try:
            response = self.session.get(photo_url, timeout=self.config.http_timeout_seconds)
            response.raise_for_status()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, requests.RequestException) as exc:
            self.logger.warning(
                "Failed to download digest photo from %s: %s: %s",
                photo_url,
                exc.__class__.__name__,
                exc,
            )
            raise TelegramError(f"Failed to download digest photo: {photo_url}") from exc

        content_type = str(response.headers.get("Content-Type", "image/jpeg")).strip() or "image/jpeg"
        return DownloadedPhoto(
            source_url=photo_url,
            filename=self._guess_photo_filename(photo_url, content_type),
            content_type=content_type,
            content=response.content,
        )

    def _download_photo_batch(self, photo_urls: list[str]) -> list[DownloadedPhoto]:
        return [self._download_photo(photo_url) for photo_url in photo_urls]

    def send_telegram_photo(self, photo_url: str, caption: str | None = None) -> None:
        if self.config.dry_run:
            self.logger.info("[DRY RUN] Telegram photo: %s", photo_url)
            if caption:
                self.logger.info("[DRY RUN] Telegram photo caption:\n%s", caption)
            return
        if not self.config.tg_bot_token or not self.config.tg_chat_id:
            raise TelegramError("Telegram token/chat is not configured.")

        photo = self._download_photo(photo_url)
        data = {"chat_id": self.config.tg_chat_id}
        if caption:
            data["caption"] = caption
        files = {
            "photo": (
                photo.filename,
                io.BytesIO(photo.content),
                photo.content_type,
            )
        }
        self._send_telegram_request(
            "sendPhoto",
            data,
            files=files,
            error_context=f"url={photo.source_url}",
        )
        self.last_telegram_send_monotonic = self.monotonic()

    def send_telegram_media_group(self, photo_urls: list[str], caption: str | None = None) -> None:
        if not photo_urls:
            return
        if self.config.dry_run:
            self.logger.info("[DRY RUN] Telegram media group: %s", photo_urls)
            if caption:
                self.logger.info("[DRY RUN] Telegram media group caption:\n%s", caption)
            return
        if not self.config.tg_bot_token or not self.config.tg_chat_id:
            raise TelegramError("Telegram token/chat is not configured.")

        for index in range(0, len(photo_urls), TELEGRAM_MEDIA_GROUP_LIMIT):
            chunk = photo_urls[index : index + TELEGRAM_MEDIA_GROUP_LIMIT]
            downloaded = self._download_photo_batch(chunk)
            media: list[dict[str, str]] = []
            files: dict[str, tuple[str, io.BytesIO, str]] = {}
            for media_index, photo in enumerate(downloaded):
                attachment_name = f"photo{media_index}"
                item: dict[str, str] = {"type": "photo", "media": f"attach://{attachment_name}"}
                if index == 0 and media_index == 0 and caption:
                    item["caption"] = caption
                media.append(item)
                files[attachment_name] = (
                    photo.filename,
                    io.BytesIO(photo.content),
                    photo.content_type,
                )
            self._send_telegram_request(
                "sendMediaGroup",
                {"chat_id": self.config.tg_chat_id, "media": json.dumps(media)},
                files=files,
                error_context=f"batch_size={len(chunk)} urls={', '.join(chunk)}",
            )
            self.last_telegram_send_monotonic = self.monotonic()

    def _get_last_tg_update_id(self) -> int:
        raw = self.state.get_meta("last_tg_update_id", "0") or "0"
        try:
            return int(raw)
        except ValueError:
            return 0

    def _is_status_command(self, text: str) -> bool:
        normalized = normalize_whitespace(text)
        return bool(re.match(r"^/status(?:@[\w_]+)?(?:\s|$)", normalized, re.IGNORECASE))

    def _is_digest_command(self, text: str) -> bool:
        normalized = normalize_whitespace(text)
        return bool(re.match(r"^/digest(?:@[\w_]+)?(?:\s|$)", normalized, re.IGNORECASE))

    def _parse_last_daily_digest_date_sent(self) -> date | None:
        raw = self.state.get_meta("last_daily_digest_date_sent")
        if not raw:
            return None
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None

    def _build_status_message(self) -> str:
        last_check = self._format_iso_timestamp_for_status(self.state.get_meta("last_check_at"))
        owner_id = self.state.get_meta("last_checked_owner_id")
        post_id = self.state.get_meta("last_checked_post_id")
        last_post = "n/a"
        if owner_id and post_id:
            last_post = f"https://vk.com/wall{owner_id}_{post_id}"
        digest_enabled = "yes" if self.config.enable_daily_digest else "no"
        last_digest = self.state.get_meta("last_daily_digest_date_sent", "n/a") or "n/a"
        next_digest = (
            self._format_iso_timestamp_for_status(self.next_daily_digest_at.isoformat())
            if self.next_daily_digest_at is not None
            else "n/a"
        )
        return "\n".join(
            [
                "🟢 Running: yes",
                f"🕒 Last check: {last_check}",
                f"🔗 Last checked post: {last_post}",
                f"Daily digest enabled: {digest_enabled}",
                f"Last daily digest date: {last_digest}",
                f"Next daily digest at: {next_digest}",
            ]
        )

    def handle_status_command(self, chat_id: str) -> None:
        if not self.config.tg_chat_id or chat_id != self.config.tg_chat_id:
            return
        self.send_telegram_message(self._build_status_message())

    def _source_link(self, owner_id: int | None) -> str:
        if self.config.vk_domain:
            return f"https://vk.com/{self.config.vk_domain}"
        if owner_id is None:
            owner_id = self.config.vk_owner_id
        if owner_id is None:
            return "https://vk.com"
        return f"https://vk.com/wall{owner_id}"

    def _section_number(self, index: int) -> str:
        if 1 <= index <= 9:
            return f"{index}\uFE0F\u20E3"
        return f"{index}."

    def _split_long_text(self, text: str, limit: int) -> list[str]:
        if len(text) <= limit:
            return [text]
        chunks: list[str] = []
        current = ""
        for line in text.split("\n"):
            candidate = line if not current else f"{current}\n{line}"
            if len(candidate) <= limit:
                current = candidate
                continue
            if current:
                chunks.append(current)
                current = ""
            if len(line) <= limit:
                current = line
                continue
            start = 0
            while start < len(line):
                end = start + limit
                chunks.append(line[start:end])
                start = end
        if current:
            chunks.append(current)
        return chunks

    def _build_digest_text_messages(self, entries: list[DailyDigestPost], source_link: str) -> list[str]:
        sections = [
            f"{self._section_number(index)}\n{entry.text}"
            for index, entry in enumerate(entries, start=1)
        ]
        messages: list[str] = []
        current = ""
        for section in sections:
            if len(section) > TELEGRAM_TEXT_LIMIT:
                if current:
                    messages.append(current)
                    current = ""
                messages.extend(self._split_long_text(section, TELEGRAM_TEXT_LIMIT))
                continue
            candidate = section if not current else f"{current}\n\n{section}"
            if len(candidate) <= TELEGRAM_TEXT_LIMIT:
                current = candidate
                continue
            if current:
                messages.append(current)
            current = section
        if current:
            messages.append(current)
        if not messages:
            messages = [source_link]
        else:
            suffix = f"\n\n{source_link}"
            if len(messages[-1]) + len(suffix) <= TELEGRAM_TEXT_LIMIT:
                messages[-1] = f"{messages[-1]}{suffix}"
            else:
                messages.append(source_link)
        return messages

    def _collect_all_digest_photo_urls(self, entries: list[DailyDigestPost]) -> list[str]:
        photo_urls: list[str] = []
        for entry in entries:
            photo_urls.extend(entry.photo_urls)
        return photo_urls

    def send_daily_digest_entries(self, entries: list[DailyDigestPost], source_link: str) -> bool:
        if not entries:
            return False

        text_messages = self._build_digest_text_messages(entries, source_link)
        all_photo_urls = self._collect_all_digest_photo_urls(entries)
        can_combine_single_post = (
            len(entries) == 1
            and bool(all_photo_urls)
            and len(text_messages) == 1
            and len(text_messages[0]) <= TELEGRAM_CAPTION_LIMIT
        )

        if can_combine_single_post:
            if len(all_photo_urls) == 1:
                self.send_telegram_photo(all_photo_urls[0], caption=text_messages[0])
            else:
                self.send_telegram_media_group(all_photo_urls, caption=text_messages[0])
            return True

        for text in text_messages:
            self.send_telegram_message(text)
        if all_photo_urls:
            if len(all_photo_urls) == 1:
                self.send_telegram_photo(all_photo_urls[0])
            else:
                self.send_telegram_media_group(all_photo_urls)
        return True

    def _entries_from_posts(self, posts: list[dict[str, Any]]) -> list[DailyDigestPost]:
        entries: list[DailyDigestPost] = []
        seen: set[tuple[int, int]] = set()
        for post in posts:
            entry = self.build_daily_digest_post(post)
            if entry is None:
                continue
            key = (entry.owner_id, entry.post_id)
            if key in seen:
                continue
            seen.add(key)
            entries.append(entry)
        entries.sort(key=lambda item: (item.unix_ts, item.post_id))
        return entries

    def _daily_digest_entries_for_date(self, target_date: date) -> list[DailyDigestPost]:
        digest_key = target_date.isoformat()
        if digest_key in self.daily_digest_buffer:
            entries = list(self.daily_digest_buffer[digest_key].values())
            entries.sort(key=lambda item: (item.unix_ts, item.post_id))
            return entries
        return self._entries_from_posts(self.fetch_posts_for_local_date(target_date))

    def _mark_daily_digest_sent(self, target_date: date) -> None:
        self.state.set_meta("last_daily_digest_date_sent", target_date.isoformat(), immediate=True)
        self.state.commit_with_pending_meta()

    def handle_digest_command(self, chat_id: str) -> None:
        if not self.config.tg_chat_id or chat_id != self.config.tg_chat_id:
            return
        target_date = self._now_local().date()
        entries = self._entries_from_posts(self.fetch_posts_for_local_date(target_date))
        if not entries:
            self.send_telegram_message("VK digest: no posts for the current day yet.")
            return
        self.send_daily_digest_entries(entries, self._source_link(entries[0].owner_id))

    def handle_telegram_update(self, update: dict[str, Any]) -> None:
        message = update.get("message")
        if not isinstance(message, dict):
            return
        chat = message.get("chat")
        if not isinstance(chat, dict):
            return
        chat_id = str(chat.get("id", ""))
        text = message.get("text")
        if not isinstance(text, str):
            return
        if self._is_status_command(text):
            self.handle_status_command(chat_id)
        elif self._is_digest_command(text):
            self.handle_digest_command(chat_id)

    def poll_telegram_updates_once(self) -> None:
        if not self.config.tg_bot_token or not self.config.tg_chat_id:
            self.logger.warning("Skipping getUpdates polling: TG_BOT_TOKEN/TG_CHAT_ID is not configured.")
            return

        url = f"{TG_API_BASE}/bot{self.config.tg_bot_token}/getUpdates"
        offset = self._get_last_tg_update_id() + 1
        params = {"offset": offset, "timeout": 0, "allowed_updates": json.dumps(["message"])}

        try:
            response = self.session.get(url, params=params, timeout=self.config.http_timeout_seconds)
            response.raise_for_status()
            payload = response.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, requests.RequestException) as exc:
            self.logger.warning("Telegram getUpdates failed: %s", exc)
            return
        except ValueError as exc:
            self.logger.warning("Telegram getUpdates invalid JSON response: %s", exc)
            return

        if not payload.get("ok", False):
            self.logger.warning("Telegram getUpdates API error: %s", payload)
            return

        updates = payload.get("result", [])
        if not isinstance(updates, list):
            self.logger.warning("Telegram getUpdates returned invalid result type.")
            return

        max_update_id: int | None = None
        for update in updates:
            if not isinstance(update, dict):
                continue
            update_id = update.get("update_id")
            if isinstance(update_id, int):
                max_update_id = update_id if max_update_id is None else max(max_update_id, update_id)
            try:
                self.handle_telegram_update(update)
            except Exception as exc:  # pragma: no cover - defensive guard
                self.logger.warning("Failed to handle Telegram update: %s", exc)

        if max_update_id is not None:
            self.state.set_meta("last_tg_update_id", str(max_update_id), immediate=False)
            self.state.commit_with_pending_meta()

    def maybe_send_recovery_message(self) -> bool:
        was_unclean, last_error = self.state.start_runtime()
        if not was_unclean:
            return False

        message = "VK monitor recovered after unclean stop."
        if last_error:
            message += f"\nLast error: {last_error[:500]}"
        self.send_telegram_message(message)
        return True

    def process_once(self, persist_state: bool = True) -> CheckResult:
        posts = self.fetch_vk_posts()
        if not posts:
            self.logger.info("VK returned no posts.")
            return CheckResult(sent_count=0, last_checked_owner_id=None, last_checked_post_id=None)

        owner_id = int(posts[0].get("owner_id", self.config.vk_owner_id))
        last_seen = self.state.get_last_seen(owner_id)
        post_ids = [int(post.get("id", 0)) for post in posts if "id" in post]
        if not post_ids:
            self.logger.info("No valid post IDs in VK response.")
            return CheckResult(sent_count=0, last_checked_owner_id=owner_id, last_checked_post_id=None)

        if last_seen is None:
            baseline = max(post_ids)
            if persist_state:
                self.logger.info("State not initialized for owner %s, baseline=%s.", owner_id, baseline)
                if not self.config.dry_run:
                    self.state.set_last_seen(owner_id, baseline, commit=True)
                return CheckResult(
                    sent_count=0,
                    last_checked_owner_id=owner_id,
                    last_checked_post_id=baseline,
                )
            last_seen = min(post_ids) - 1
            self.logger.info(
                "State not initialized for owner %s; running read-only scan over fetched posts.",
                owner_id,
            )

        new_posts = [post for post in posts if int(post["id"]) > last_seen]
        if not self.config.catch_up and new_posts:
            newest = max(new_posts, key=lambda post: int(post["id"]))
            new_posts = [newest]
        new_posts.sort(key=lambda post: int(post["id"]))
        last_checked_post_id = max(int(post["id"]) for post in new_posts) if new_posts else max(post_ids)

        sent_count = 0
        for post in new_posts:
            post_id = int(post["id"])
            if self.config.enable_daily_digest and persist_state:
                self._buffer_daily_digest_post(post)

            if not self.config.enable_instant_alerts:
                if persist_state and not self.config.dry_run:
                    self.state.set_last_seen(owner_id, post_id, commit=True)
                continue

            if self.state.is_notified(owner_id, post_id):
                self.logger.info("Post %s already notified; skipping duplicate.", post_id)
                if persist_state and not self.config.dry_run:
                    self.state.set_last_seen(owner_id, post_id, commit=True)
                continue

            matched_terms = self.match_post(post)
            if matched_terms:
                message = self.build_post_message(post, matched_terms)
                self.send_telegram_message(message)
                sent_count += 1
                if persist_state and not self.config.dry_run:
                    self.state.mark_notified(owner_id, post_id, commit=False)
                    self.state.set_last_seen(owner_id, post_id, commit=False)
                    self.state.commit_with_pending_meta()
            else:
                if persist_state and not self.config.dry_run:
                    self.state.set_last_seen(owner_id, post_id, commit=True)

        return CheckResult(
            sent_count=sent_count,
            last_checked_owner_id=owner_id,
            last_checked_post_id=last_checked_post_id,
        )

    def check_once_with_backoff(self, max_attempts: int = 5, persist_state: bool = True) -> int:
        attempt = 0
        while True:
            try:
                return self.process_once(persist_state=persist_state).sent_count
            except VKTransientError as exc:
                if attempt >= max_attempts - 1:
                    raise
                delay = BACKOFF_SCHEDULE_SECONDS[min(attempt, len(BACKOFF_SCHEDULE_SECONDS) - 1)]
                self.logger.warning("Transient VK error: %s. Retrying in %ss.", exc, delay)
                self.sleep(delay)
                attempt += 1

    def _schedule_next_daily_digest(self) -> None:
        if not self.config.enable_daily_digest:
            self.next_daily_digest_at = None
            return
        now_local = self._now_local()
        scheduled_local = datetime.combine(
            now_local.date(),
            self.daily_digest_time_of_day,
            tzinfo=self._local_tz(),
        )
        if now_local >= scheduled_local:
            scheduled_local += timedelta(days=1)
        self.next_daily_digest_at = scheduled_local.astimezone(timezone.utc)

    def initialize_daily_digest_schedule(self) -> None:
        if not self.config.enable_daily_digest:
            self.next_daily_digest_at = None
            self.pending_daily_digest_date = None
            return
        yesterday = self._now_local().date() - timedelta(days=1)
        last_sent = self._parse_last_daily_digest_date_sent()
        if last_sent != yesterday:
            self.pending_daily_digest_date = yesterday
            self.next_daily_digest_at = self._now_utc()
            return
        self.pending_daily_digest_date = None
        self._schedule_next_daily_digest()

    def process_daily_digest_due(self) -> None:
        if not self.config.enable_daily_digest:
            return
        target_date = self.pending_daily_digest_date
        if target_date is None:
            target_date = self._now_local().date() - timedelta(days=1)
        entries = self._daily_digest_entries_for_date(target_date)
        if entries:
            self.send_daily_digest_entries(entries, self._source_link(entries[0].owner_id))
        if not self.config.dry_run:
            self._mark_daily_digest_sent(target_date)
        self._clear_daily_digest_buffer_up_to(target_date)
        self.pending_daily_digest_date = None
        self._schedule_next_daily_digest()


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
    tg_updates_interval = int(env("TG_UPDATES_INTERVAL_SECONDS", "30"))
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
        tg_updates_interval_seconds=tg_updates_interval,
        enable_instant_alerts=parse_bool(env("ENABLE_INSTANT_ALERTS"), True),
        enable_daily_digest=parse_bool(env("ENABLE_DAILY_DIGEST"), False),
        daily_digest_time=env("DAILY_DIGEST_TIME", "08:00") or "08:00",
        digest_line_excludes=parse_csv_list(env("DIGEST_LINE_EXCLUDES")),
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
    sent = monitor.check_once_with_backoff(persist_state=False)
    print(f"check-once complete. sent={sent}")
    return 0


def command_run(monitor: Monitor) -> int:
    _install_sigterm_handler()
    monitor.maybe_send_recovery_message()
    monitor.initialize_daily_digest_schedule()
    logger = logging.getLogger("vk_wall_monitor")

    backoff_index = 0
    now_monotonic = monitor.monotonic()
    next_vk_due = now_monotonic
    next_tg_due = now_monotonic
    next_digest_due = (
        now_monotonic
        if monitor.next_daily_digest_at is not None and monitor.next_daily_digest_at <= monitor._now_utc()
        else (
            now_monotonic + max(0.0, (monitor.next_daily_digest_at - monitor._now_utc()).total_seconds())
            if monitor.next_daily_digest_at is not None
            else float("inf")
        )
    )
    monitor.state.set_meta("next_check_at", datetime.now(timezone.utc).isoformat(), immediate=False)

    try:
        while True:
            now_monotonic = monitor.monotonic()

            if now_monotonic >= next_vk_due:
                next_delay = monitor.config.interval_seconds
                try:
                    result = monitor.process_once()
                    backoff_index = 0
                    if result.last_checked_owner_id is not None and result.last_checked_post_id is not None:
                        monitor.state.set_meta(
                            "last_checked_owner_id",
                            str(result.last_checked_owner_id),
                            immediate=False,
                        )
                        monitor.state.set_meta(
                            "last_checked_post_id",
                            str(result.last_checked_post_id),
                            immediate=False,
                        )
                except VKTransientError as exc:
                    delay = BACKOFF_SCHEDULE_SECONDS[min(backoff_index, len(BACKOFF_SCHEDULE_SECONDS) - 1)]
                    backoff_index = min(backoff_index + 1, len(BACKOFF_SCHEDULE_SECONDS) - 1)
                    monitor.state.set_last_error(str(exc))
                    logger.warning("Transient VK error in run loop: %s. Sleeping %ss.", exc, delay)
                    next_delay = delay

                finished_at = datetime.now(timezone.utc)
                monitor.state.set_meta("last_check_at", finished_at.isoformat(), immediate=False)
                monitor.state.set_meta(
                    "next_check_at",
                    (finished_at + timedelta(seconds=next_delay)).isoformat(),
                    immediate=False,
                )
                next_vk_due = monitor.monotonic() + next_delay

            now_monotonic = monitor.monotonic()
            if now_monotonic >= next_tg_due:
                monitor.poll_telegram_updates_once()
                next_tg_due = monitor.monotonic() + monitor.config.tg_updates_interval_seconds

            now_monotonic = monitor.monotonic()
            if now_monotonic >= next_digest_due:
                try:
                    monitor.process_daily_digest_due()
                    if monitor.next_daily_digest_at is None:
                        next_digest_due = float("inf")
                    else:
                        next_digest_due = monitor.monotonic() + max(
                            0.0,
                            (monitor.next_daily_digest_at - monitor._now_utc()).total_seconds(),
                        )
                except VKMonitorError as exc:
                    monitor.state.set_last_error(str(exc))
                    logger.warning("Daily digest failed: %s. Retrying in %ss.", exc, DAILY_DIGEST_RETRY_SECONDS)
                    next_digest_due = monitor.monotonic() + DAILY_DIGEST_RETRY_SECONDS

            sleep_for = max(0.0, min(next_vk_due, next_tg_due, next_digest_due) - monitor.monotonic())
            if sleep_for > 0:
                monitor.sleep(sleep_for)
    except KeyboardInterrupt:
        logger.info("Received stop signal, shutting down cleanly.")
        monitor.state.mark_clean_shutdown()
        return 0
    except Exception as exc:
        monitor.state.set_last_error(str(exc))
        monitor.state.commit_with_pending_meta()
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
        monitor.state.commit_with_pending_meta()
        print(str(exc), file=sys.stderr)
        return 1
    except VKMonitorError as exc:
        monitor.state.set_last_error(str(exc))
        monitor.state.commit_with_pending_meta()
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        monitor.close()


if __name__ == "__main__":
    raise SystemExit(main())
