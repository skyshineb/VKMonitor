from __future__ import annotations

import argparse
import logging
import os
import sys
import json
import tempfile
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from vk_wall_monitor.app import Config, Monitor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Local helper to test VK photo download and Telegram upload for digest media."
    )
    parser.add_argument(
        "--mode",
        choices=["download-only", "photo-only", "photo-only-url", "digest-single"],
        default="digest-single",
        help="download-only checks VK image download, photo-only uploads via local download, photo-only-url uses Telegram external URLs, digest-single uses the digest send path.",
    )
    parser.add_argument("--vk-access-token", default=os.getenv("VK_ACCESS_TOKEN"))
    parser.add_argument("--vk-domain", default=os.getenv("VK_DOMAIN"))
    parser.add_argument("--vk-owner-id", type=int, default=int(os.getenv("VK_OWNER_ID", "0") or 0) or None)
    parser.add_argument("--tg-bot-token", default=os.getenv("TG_BOT_TOKEN"))
    parser.add_argument("--tg-chat-id", default=os.getenv("TG_CHAT_ID"))
    parser.add_argument("--timezone", default=os.getenv("TIMEZONE", "Europe/Berlin"))
    parser.add_argument(
        "--date",
        dest="target_date",
        default=None,
        help="Local date in YYYY-MM-DD. If omitted, scan recent posts until one with photo is found.",
    )
    parser.add_argument(
        "--post-id",
        type=int,
        default=None,
        help="Specific VK post id to select from fetched candidates.",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=3,
        help="How many wall.get pages to scan when --date is not provided.",
    )
    parser.add_argument(
        "--count-per-page",
        type=int,
        default=100,
        help="VK wall.get page size for recent scan mode.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def require(value: str | int | None, name: str) -> str | int:
    if value is None or value == "":
        raise ValueError(f"{name} is required.")
    return value


def build_config(args: argparse.Namespace) -> Config:
    vk_access_token = str(require(args.vk_access_token, "VK access token"))
    tg_bot_token = str(require(args.tg_bot_token, "Telegram bot token"))
    tg_chat_id = str(require(args.tg_chat_id, "Telegram chat id"))
    vk_domain = args.vk_domain or None
    vk_owner_id = args.vk_owner_id
    if not vk_domain and vk_owner_id is None:
        raise ValueError("Provide either --vk-domain or --vk-owner-id.")
    if vk_domain and vk_owner_id is not None:
        raise ValueError("Use only one of --vk-domain or --vk-owner-id.")

    state_path = Path(tempfile.gettempdir()) / "vk_monitor_test_digest_media.sqlite"
    return Config(
        vk_access_token=vk_access_token,
        tg_bot_token=tg_bot_token,
        tg_chat_id=tg_chat_id,
        vk_domain=vk_domain,
        vk_owner_id=vk_owner_id,
        interval_seconds=30,
        count=max(1, min(args.count_per_page, 100)),
        wall_filter="owner",
        vk_api_version="5.199",
        keywords=[],
        keywords_regex=None,
        mode="any",
        dry_run=False,
        catch_up=True,
        state_path=state_path,
        timezone_name=args.timezone,
        tg_updates_interval_seconds=30,
        enable_instant_alerts=False,
        enable_daily_digest=True,
        daily_digest_time="08:00",
        digest_line_excludes=[],
        digest_image_mode="url",
    )


def fetch_recent_posts_with_photos(monitor: Monitor, pages: int, count_per_page: int) -> list[dict]:
    posts: list[dict] = []
    for page in range(max(1, pages)):
        batch = monitor.fetch_vk_posts(offset=page * count_per_page, count=count_per_page)
        if not batch:
            break
        posts.extend(batch)
        if any(monitor.build_daily_digest_post(post) and monitor.build_daily_digest_post(post).photo_urls for post in batch):
            break
    return posts


def select_entry(monitor: Monitor, posts: list[dict], post_id: int | None):
    entries = [monitor.build_daily_digest_post(post) for post in posts]
    entries = [entry for entry in entries if entry is not None and entry.photo_urls]
    if post_id is not None:
        entries = [entry for entry in entries if entry.post_id == post_id]
    if not entries:
        return None
    entries.sort(key=lambda item: (item.unix_ts, item.post_id), reverse=True)
    return entries[0]


def print_entry(entry) -> None:
    print(f"Selected post_id={entry.post_id} owner_id={entry.owner_id} photo_count={len(entry.photo_urls)}")
    print(f"Digest date local={entry.digest_date_local}")
    print("Photo URLs:")
    for url in entry.photo_urls:
        print(f"  - {url}")
    preview = entry.text.replace("\n", " | ")
    print(f"Text preview={preview[:200]}")


def send_photo_urls_direct(monitor: Monitor, photo_urls: list[str], caption: str | None = None) -> None:
    if len(photo_urls) == 1:
        data = {"chat_id": monitor.config.tg_chat_id, "photo": photo_urls[0]}
        if caption:
            data["caption"] = caption
        monitor._send_telegram_request(
            "sendPhoto",
            data,
            error_context=f"url={photo_urls[0]}",
        )
        return

    for index in range(0, len(photo_urls), 10):
        chunk = photo_urls[index : index + 10]
        media: list[dict[str, str]] = []
        for media_index, photo_url in enumerate(chunk):
            item: dict[str, str] = {"type": "photo", "media": photo_url}
            if index == 0 and media_index == 0 and caption:
                item["caption"] = caption
            media.append(item)
        monitor._send_telegram_request(
            "sendMediaGroup",
            {"chat_id": monitor.config.tg_chat_id, "media": json.dumps(media)},
            error_context=f"legacy_url_batch={', '.join(chunk)}",
        )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        config = build_config(args)
        config.validate("run")
    except Exception as exc:
        print(f"Configuration error: {exc}")
        return 2

    monitor = Monitor(config=config)
    try:
        if args.target_date:
            target_date = date.fromisoformat(args.target_date)
            posts = monitor.fetch_posts_for_local_date(target_date)
            print(f"Fetched {len(posts)} posts for local date {target_date.isoformat()}.")
        else:
            posts = fetch_recent_posts_with_photos(monitor, args.pages, config.count)
            print(f"Fetched {len(posts)} recent posts across up to {args.pages} page(s).")

        entry = select_entry(monitor, posts, args.post_id)
        if entry is None:
            print("No post with photos found for the selected scope.")
            return 1

        print_entry(entry)

        if args.mode == "download-only":
            photos = monitor._download_photo_batch(entry.photo_urls)
            print(f"Downloaded {len(photos)} file(s):")
            for photo in photos:
                print(
                    f"  - {photo.filename} content_type={photo.content_type} bytes={len(photo.content)} url={photo.source_url}"
                )
            return 0

        if args.mode == "photo-only":
            if len(entry.photo_urls) == 1:
                monitor.send_telegram_photo(entry.photo_urls[0], caption=f"Photo upload test for post {entry.post_id}")
            else:
                monitor.send_telegram_media_group(entry.photo_urls, caption=f"Photo upload test for post {entry.post_id}")
            print("Photo-only upload sent to Telegram.")
            return 0

        if args.mode == "photo-only-url":
            send_photo_urls_direct(
                monitor,
                entry.photo_urls,
                caption=f"Legacy URL upload test for post {entry.post_id}",
            )
            print("Photo-only URL upload sent to Telegram.")
            return 0

        monitor.send_daily_digest_entries([entry], monitor._source_link(entry.owner_id))
        print("Digest-single upload sent to Telegram.")
        return 0
    finally:
        monitor.close()


if __name__ == "__main__":
    raise SystemExit(main())
