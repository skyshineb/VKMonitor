from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs

import responses

from vk_wall_monitor.app import Config, Monitor, StateStore, command_run


VK_URL = "https://api.vk.com/method/wall.get"


def make_config(tmp_path: Path, **overrides) -> Config:
    base = Config(
        vk_access_token="vk-token",
        tg_bot_token="tg-token",
        tg_chat_id="123456",
        vk_domain=None,
        vk_owner_id=-123,
        interval_seconds=30,
        count=10,
        wall_filter="owner",
        vk_api_version="5.199",
        keywords=["alert"],
        keywords_regex=None,
        mode="any",
        dry_run=False,
        catch_up=True,
        state_path=tmp_path / "state.sqlite",
        timezone_name="Europe/Berlin",
        tg_updates_interval_seconds=30,
        enable_instant_alerts=True,
        enable_daily_digest=False,
        daily_digest_time="08:00",
        digest_line_excludes=[],
        digest_image_mode="url",
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def tg_url(config: Config) -> str:
    return f"https://api.telegram.org/bot{config.tg_bot_token}/sendMessage"


def tg_get_updates_url(config: Config) -> str:
    return f"https://api.telegram.org/bot{config.tg_bot_token}/getUpdates"


def tg_media_group_url(config: Config) -> str:
    return f"https://api.telegram.org/bot{config.tg_bot_token}/sendMediaGroup"


def tg_photo_url(config: Config) -> str:
    return f"https://api.telegram.org/bot{config.tg_bot_token}/sendPhoto"


def count_calls(calls, marker: str) -> int:
    return sum(1 for call in calls if marker in call.request.url)


def extract_message_text(call) -> str:
    body = call.request.body
    parsed = parse_qs(body.decode() if isinstance(body, bytes) else body)
    return parsed["text"][0]


def extract_media_payload(call) -> list[dict[str, str]]:
    body = call.request.body
    parsed = parse_qs(body.decode() if isinstance(body, bytes) else body)
    return json.loads(parsed["media"][0])


def body_text(call) -> str:
    body = call.request.body
    if isinstance(body, bytes):
        return body.decode("utf-8", errors="ignore")
    return body


def utc_ts(year: int, month: int, day: int, hour: int = 10, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp())


@responses.activate
def test_check_once_twice_same_vk_response_sends_once(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_last_seen(-123, 1)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    vk_payload = {
        "response": {
            "items": [{"id": 2, "owner_id": -123, "date": 1_700_000_001, "text": "ALERT: test"}]
        }
    }
    responses.add(responses.GET, VK_URL, json=vk_payload, status=200)
    responses.add(responses.POST, tg_url(config), json={"ok": True, "result": {"message_id": 1}}, status=200)
    responses.add(responses.GET, VK_URL, json=vk_payload, status=200)

    monitor.check_once_with_backoff()
    monitor.check_once_with_backoff()

    assert count_calls(responses.calls, "/sendMessage") == 1
    monitor.close()


@responses.activate
def test_new_post_sends_valid_vk_link(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_last_seen(-123, 1)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    responses.add(
        responses.GET,
        VK_URL,
        json={
            "response": {
                "items": [
                    {
                        "id": 2,
                        "owner_id": -123,
                        "date": 1_700_000_001,
                        "text": "Alert body text",
                    }
                ]
            }
        },
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 2}},
        status=200,
    )

    monitor.check_once_with_backoff()

    send_calls = [call for call in responses.calls if "/sendMessage" in call.request.url]
    assert len(send_calls) == 1
    body = send_calls[0].request.body
    parsed = parse_qs(body.decode() if isinstance(body, bytes) else body)
    text = parsed["text"][0]
    assert "https://vk.com/wall-123_2" in text
    monitor.close()


@responses.activate
def test_vk_rate_limit_uses_backoff(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_last_seen(-123, 0)
    sleeps: list[float] = []
    monitor = Monitor(config=config, state=state, sleeper=lambda s: sleeps.append(s))

    responses.add(
        responses.GET,
        VK_URL,
        json={"error": {"error_code": 6, "error_msg": "Too many requests per second"}},
        status=200,
    )
    responses.add(
        responses.GET,
        VK_URL,
        json={"response": {"items": [{"id": 1, "owner_id": -123, "date": 1_700_000_001, "text": "no hit"}]}},
        status=200,
    )

    monitor.check_once_with_backoff()

    assert sleeps[0] == 5
    monitor.close()


@responses.activate
def test_telegram_429_retries_with_retry_after(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_last_seen(-123, 1)
    sleeps: list[float] = []
    monitor = Monitor(config=config, state=state, sleeper=lambda s: sleeps.append(s))

    responses.add(
        responses.GET,
        VK_URL,
        json={
            "response": {
                "items": [{"id": 2, "owner_id": -123, "date": 1_700_000_001, "text": "alert with retry"}]
            }
        },
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": False, "error_code": 429, "parameters": {"retry_after": 2}},
        status=429,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 3}},
        status=200,
    )

    monitor.check_once_with_backoff()

    assert 2 in sleeps
    assert count_calls(responses.calls, "/sendMessage") == 2
    monitor.close()


@responses.activate
def test_recovery_message_only_after_unclean_stop(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    state_unclean = StateStore(config.state_path)
    state_unclean.set_meta("initialized", "1")
    state_unclean.set_meta("shutdown_clean", "0")
    state_unclean.set_meta("last_error", "boom")
    monitor_unclean = Monitor(config=config, state=state_unclean, sleeper=lambda _: None)
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 4}},
        status=200,
    )
    assert monitor_unclean.maybe_send_recovery_message() is True
    assert count_calls(responses.calls, "/sendMessage") == 1
    monitor_unclean.close()

    clean_config = make_config(tmp_path, state_path=tmp_path / "clean.sqlite")
    state_clean = StateStore(clean_config.state_path)
    state_clean.set_meta("initialized", "1")
    state_clean.set_meta("shutdown_clean", "1")
    monitor_clean = Monitor(config=clean_config, state=state_clean, sleeper=lambda _: None)
    assert monitor_clean.maybe_send_recovery_message() is False
    monitor_clean.close()


@responses.activate
def test_pinned_post_replay_not_sent(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_last_seen(-123, 10)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    responses.add(
        responses.GET,
        VK_URL,
        json={
            "response": {
                "items": [
                    {
                        "id": 5,
                        "owner_id": -123,
                        "date": 1_700_000_001,
                        "text": "alert in pinned old post",
                        "is_pinned": 1,
                    }
                ]
            }
        },
        status=200,
    )

    monitor.check_once_with_backoff()

    assert count_calls(responses.calls, "/sendMessage") == 0
    monitor.close()


@responses.activate
def test_check_once_read_only_does_not_persist_state(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_last_seen(-123, 1)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    responses.add(
        responses.GET,
        VK_URL,
        json={
            "response": {
                "items": [{"id": 2, "owner_id": -123, "date": 1_700_000_001, "text": "alert read only"}]
            }
        },
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 5}},
        status=200,
    )

    monitor.check_once_with_backoff(persist_state=False)

    assert state.get_last_seen(-123) == 1
    assert state.is_notified(-123, 2) is False
    monitor.close()


@responses.activate
def test_check_once_read_only_without_state_still_matches(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    responses.add(
        responses.GET,
        VK_URL,
        json={
            "response": {
                "items": [{"id": 2, "owner_id": -123, "date": 1_700_000_001, "text": "ALERT first check"}]
            }
        },
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 6}},
        status=200,
    )

    sent = monitor.check_once_with_backoff(persist_state=False)

    assert sent == 1
    assert state.get_last_seen(-123) is None
    assert state.is_notified(-123, 2) is False
    monitor.close()


def test_matcher_modes_regex_and_copy_history(tmp_path: Path) -> None:
    config_any = make_config(tmp_path, keywords=["foo", "bar"], mode="any")
    monitor_any = Monitor(config=config_any)
    post_any = {"id": 1, "owner_id": -123, "date": 1, "text": "xxx Foo yyy"}
    assert monitor_any.match_post(post_any) == ["foo"]
    monitor_any.close()

    config_all = make_config(tmp_path, keywords=["foo", "bar"], mode="all", state_path=tmp_path / "all.sqlite")
    monitor_all = Monitor(config=config_all)
    post_all = {"id": 1, "owner_id": -123, "date": 1, "text": "foo only"}
    assert monitor_all.match_post(post_all) == []
    post_all_hit = {"id": 2, "owner_id": -123, "date": 1, "text": "foo", "copy_history": [{"text": "bar"}]}
    assert monitor_all.match_post(post_all_hit) == ["foo", "bar"]
    monitor_all.close()

    config_regex = make_config(
        tmp_path,
        keywords=[],
        keywords_regex=r"promo\s+\d+",
        state_path=tmp_path / "regex.sqlite",
    )
    monitor_regex = Monitor(config=config_regex)
    post_regex = {"id": 1, "owner_id": -123, "date": 1, "text": "Great PROMO 42 today"}
    matched = monitor_regex.match_post(post_regex)
    assert len(matched) == 1
    assert matched[0].casefold() == "promo 42"
    monitor_regex.close()


@responses.activate
def test_status_command_in_allowed_chat_returns_core_fields(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_meta("last_check_at", "2026-03-06T10:00:00+00:00")
    state.set_meta("next_check_at", "2026-03-06T10:00:30+00:00")
    state.set_meta("last_checked_owner_id", "-123")
    state.set_meta("last_checked_post_id", "77")
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        json={
            "ok": True,
            "result": [
                {"update_id": 101, "message": {"chat": {"id": 123456}, "text": "/status"}},
            ],
        },
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 10}},
        status=200,
    )

    monitor.poll_telegram_updates_once()

    send_calls = [call for call in responses.calls if "/sendMessage" in call.request.url]
    assert len(send_calls) == 1
    text = extract_message_text(send_calls[0])
    assert "🟢 Running: yes" in text
    assert "🕒 Last check:" in text
    assert "next check:" not in text
    assert "🔗 Last checked post: https://vk.com/wall-123_77" in text
    assert state.get_meta("last_tg_update_id") == "101"
    monitor.close()


@responses.activate
def test_status_command_in_wrong_chat_is_ignored(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        json={
            "ok": True,
            "result": [
                {"update_id": 102, "message": {"chat": {"id": 999999}, "text": "/status@SomeBot"}},
            ],
        },
        status=200,
    )

    monitor.poll_telegram_updates_once()

    assert count_calls(responses.calls, "/sendMessage") == 0
    assert state.get_meta("last_tg_update_id") == "102"
    monitor.close()


@responses.activate
def test_get_updates_offset_prevents_duplicate_status_reply(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    first_update = {"update_id": 200, "message": {"chat": {"id": 123456}, "text": "/status"}}
    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        json={"ok": True, "result": [first_update]},
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 11}},
        status=200,
    )
    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        json={"ok": True, "result": []},
        status=200,
    )

    monitor.poll_telegram_updates_once()
    monitor.poll_telegram_updates_once()

    assert count_calls(responses.calls, "/sendMessage") == 1
    get_calls = [call for call in responses.calls if "/getUpdates" in call.request.url]
    assert "offset=201" in get_calls[1].request.url
    monitor.close()


@responses.activate
def test_get_updates_http_error_does_not_raise(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        body="bad gateway",
        status=502,
    )

    monitor.poll_telegram_updates_once()

    assert count_calls(responses.calls, "/sendMessage") == 0
    monitor.close()


@responses.activate
def test_status_command_without_last_post_shows_na(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_meta("last_check_at", "2026-03-06T10:00:00+00:00")
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        json={
            "ok": True,
            "result": [
                {"update_id": 300, "message": {"chat": {"id": 123456}, "text": "/status"}},
            ],
        },
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 12}},
        status=200,
    )

    monitor.poll_telegram_updates_once()

    send_calls = [call for call in responses.calls if "/sendMessage" in call.request.url]
    assert len(send_calls) == 1
    text = extract_message_text(send_calls[0])
    assert "🔗 Last checked post: n/a" in text
    monitor.close()


@responses.activate
def test_run_updates_check_schedule_and_last_checked_post(tmp_path: Path) -> None:
    config = make_config(tmp_path, interval_seconds=30, tg_updates_interval_seconds=30)
    state = StateStore(config.state_path)
    state.set_last_seen(-123, 1)
    sleeps: list[float] = []

    def stop_after_first_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        raise KeyboardInterrupt

    monitor = Monitor(config=config, state=state, sleeper=stop_after_first_sleep)

    responses.add(
        responses.GET,
        VK_URL,
        json={
            "response": {
                "items": [{"id": 2, "owner_id": -123, "date": 1_700_000_001, "text": "no keyword hit"}]
            }
        },
        status=200,
    )
    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        json={"ok": True, "result": []},
        status=200,
    )

    exit_code = command_run(monitor)

    assert exit_code == 0
    assert state.get_meta("last_checked_owner_id") == "-123"
    assert state.get_meta("last_checked_post_id") == "2"
    last_check = state.get_meta("last_check_at")
    next_check = state.get_meta("next_check_at")
    assert last_check is not None
    assert next_check is not None
    assert datetime.fromisoformat(next_check) > datetime.fromisoformat(last_check)
    assert sleeps
    monitor.close()


def test_buffered_meta_not_visible_in_db_until_flush(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_meta("last_check_at", "2026-03-06T10:00:00+00:00")

    # In-memory cache should return buffered value immediately.
    assert state.get_meta("last_check_at") == "2026-03-06T10:00:00+00:00"

    # Fresh connection should not see buffered value before flush.
    fresh = StateStore(config.state_path)
    assert fresh.get_meta("last_check_at") is None
    fresh.close()

    state.commit_with_pending_meta()
    fresh2 = StateStore(config.state_path)
    assert fresh2.get_meta("last_check_at") == "2026-03-06T10:00:00+00:00"
    fresh2.close()
    state.close()


def test_important_write_flushes_pending_meta(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_meta("last_check_at", "2026-03-06T10:00:00+00:00")
    state.set_last_seen(-123, 5, commit=True)

    fresh = StateStore(config.state_path)
    assert fresh.get_meta("last_check_at") == "2026-03-06T10:00:00+00:00"
    assert fresh.get_last_seen(-123) == 5
    fresh.close()
    state.close()


@responses.activate
def test_last_tg_update_id_persisted_on_updates(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)

    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        json={
            "ok": True,
            "result": [
                {"update_id": 501, "message": {"chat": {"id": 123456}, "text": "/status"}},
            ],
        },
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 99}},
        status=200,
    )

    monitor.poll_telegram_updates_once()
    monitor.close()

    fresh = StateStore(config.state_path)
    assert fresh.get_meta("last_tg_update_id") == "501"
    fresh.close()


def test_set_meta_noop_same_value_does_not_dirty(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state = StateStore(config.state_path)
    state.set_meta("last_check_at", "same-value")
    first_pending = dict(state._pending_meta)
    state.set_meta("last_check_at", "same-value")
    assert state._pending_meta == first_pending
    state.close()


def test_config_validate_rejects_invalid_daily_time_and_disabled_modes(tmp_path: Path) -> None:
    config_invalid_time = make_config(tmp_path, daily_digest_time="25:99")
    try:
        config_invalid_time.validate("run")
        assert False, "expected invalid daily digest time to fail"
    except ValueError as exc:
        assert "HH:MM" in str(exc)

    config_invalid_image_mode = make_config(tmp_path, digest_image_mode="broken", state_path=tmp_path / "image-mode.sqlite")
    try:
        config_invalid_image_mode.validate("run")
        assert False, "expected invalid digest image mode to fail"
    except ValueError as exc:
        assert "digest_image_mode" in str(exc)

    config_disabled = make_config(
        tmp_path,
        enable_instant_alerts=False,
        enable_daily_digest=False,
        state_path=tmp_path / "disabled.sqlite",
    )
    try:
        config_disabled.validate("run")
        assert False, "expected disabled modes to fail"
    except ValueError as exc:
        assert "At least one" in str(exc)


def test_daily_digest_filters_lines_and_collects_copy_history_photo(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        enable_daily_digest=True,
        digest_line_excludes=["sale", "spam"],
    )
    monitor = Monitor(config=config)
    post = {
        "id": 10,
        "owner_id": -123,
        "date": 1_700_000_001,
        "text": "Keep me\nSALE today\nAnother line",
        "copy_history": [
            {
                "text": "Spam line\nCopied keep",
                "attachments": [
                    {
                        "type": "photo",
                        "photo": {
                            "sizes": [
                                {"url": "https://img/small.jpg", "width": 10, "height": 10},
                                {"url": "https://img/big.jpg", "width": 100, "height": 100},
                            ]
                        },
                    }
                ],
            }
        ],
    }

    digest_post = monitor.build_daily_digest_post(post)

    assert digest_post is not None
    assert digest_post.text == "Keep me\nAnother line\nCopied keep"
    assert digest_post.photo_urls == ["https://img/big.jpg"]
    monitor.close()


@responses.activate
def test_daily_digest_buffer_is_in_memory_until_send(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        enable_instant_alerts=False,
        enable_daily_digest=True,
    )
    state = StateStore(config.state_path)
    state.set_last_seen(-123, 1)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)
    responses.add(
        responses.GET,
        VK_URL,
        json={"response": {"items": [{"id": 2, "owner_id": -123, "date": 1_700_000_001, "text": "digest text"}]}},
        status=200,
    )

    monitor.process_once()

    assert "2023-11-14" in monitor.daily_digest_buffer
    fresh = StateStore(config.state_path)
    assert fresh.get_meta("last_daily_digest_date_sent") is None
    fresh.close()
    monitor.close()


@responses.activate
def test_daily_digest_single_post_with_photo_uses_url_mode_by_default_and_persists(tmp_path: Path) -> None:
    config = make_config(tmp_path, enable_daily_digest=True)
    state = StateStore(config.state_path)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)
    target_date = date(2023, 11, 14)
    monitor.pending_daily_digest_date = target_date
    monitor.daily_digest_buffer[target_date.isoformat()] = {
        (-123, 5): monitor.build_daily_digest_post(
            {
                "id": 5,
                "owner_id": -123,
                "date": 1_700_000_001,
                "text": "Digest body",
                "attachments": [
                    {
                        "type": "photo",
                        "photo": {"sizes": [{"url": "https://img/1.jpg", "width": 50, "height": 50}]},
                    }
                ],
            }
        )
    }
    responses.add(
        responses.POST,
        tg_photo_url(config),
        json={"ok": True, "result": {"message_id": 500}},
        status=200,
    )

    monitor.process_daily_digest_due()

    photo_calls = [call for call in responses.calls if "/sendPhoto" in call.request.url]
    assert len(photo_calls) == 1
    assert "name=\"photo\"" not in body_text(photo_calls[0])
    photo_body = parse_qs(body_text(photo_calls[0]))
    assert photo_body["photo"][0] == "https://img/1.jpg"
    assert "1️⃣ Digest body" in photo_body["caption"][0]
    assert "Digest body" in photo_body["caption"][0]
    assert "https://vk.com/wall-123" in photo_body["caption"][0]
    assert count_calls(responses.calls, "https://img/1.jpg") == 0
    assert count_calls(responses.calls, "/sendMessage") == 0
    assert count_calls(responses.calls, "/sendMediaGroup") == 0
    assert state.get_meta("last_daily_digest_date_sent") == "2023-11-14"
    assert target_date.isoformat() not in monitor.daily_digest_buffer
    monitor.close()


@responses.activate
def test_daily_digest_single_post_multiple_photos_uses_media_group_with_caption(tmp_path: Path) -> None:
    config = make_config(tmp_path, enable_daily_digest=True)
    monitor = Monitor(config=config, sleeper=lambda _: None)
    entry = monitor.build_daily_digest_post(
        {
            "id": 1,
            "owner_id": -123,
            "date": 1_700_000_001,
            "text": "Single post text",
            "attachments": [
                {
                    "type": "photo",
                    "photo": {"sizes": [{"url": f"https://img/single-{index}.jpg", "width": 10, "height": 10}]},
                }
                for index in range(1, 4)
            ],
        }
    )
    responses.add(
        responses.POST,
        tg_media_group_url(config),
        json={"ok": True, "result": []},
        status=200,
    )

    monitor.send_daily_digest_entries([entry] if entry is not None else [], "https://vk.com/wall-123")

    media_calls = [call for call in responses.calls if "/sendMediaGroup" in call.request.url]
    assert len(media_calls) == 1
    media = extract_media_payload(media_calls[0])
    assert media[0]["media"] == "https://img/single-1.jpg"
    assert all(not item["media"].startswith("attach://") for item in media)
    assert media[0]["caption"]
    assert "Single post text" in media[0]["caption"]
    monitor.close()


@responses.activate
def test_daily_digest_upload_mode_downloads_photo_before_sending(tmp_path: Path) -> None:
    config = make_config(tmp_path, enable_daily_digest=True, digest_image_mode="upload")
    monitor = Monitor(config=config, sleeper=lambda _: None)
    entry = monitor.build_daily_digest_post(
        {
            "id": 6,
            "owner_id": -123,
            "date": 1_700_000_001,
            "text": "Upload mode text",
            "attachments": [
                {
                    "type": "photo",
                    "photo": {"sizes": [{"url": "https://img/upload-mode.jpg", "width": 50, "height": 50}]},
                }
            ],
        }
    )
    responses.add(
        responses.GET,
        "https://img/upload-mode.jpg",
        body=b"jpg-data",
        headers={"Content-Type": "image/jpeg"},
        status=200,
    )
    responses.add(
        responses.POST,
        tg_photo_url(config),
        json={"ok": True, "result": {"message_id": 501}},
        status=200,
    )

    monitor.send_daily_digest_entries([entry] if entry is not None else [], "https://vk.com/wall-123")

    photo_calls = [call for call in responses.calls if "/sendPhoto" in call.request.url]
    assert len(photo_calls) == 1
    assert "name=\"photo\"" in body_text(photo_calls[0])
    assert count_calls(responses.calls, "https://img/upload-mode.jpg") == 1
    monitor.close()


@responses.activate
def test_daily_digest_multiple_posts_sends_text_and_splits_media_groups(tmp_path: Path) -> None:
    config = make_config(tmp_path, enable_daily_digest=True)
    monitor = Monitor(config=config, sleeper=lambda _: None)
    entries = [
        monitor.build_daily_digest_post(
            {
                "id": 1,
                "owner_id": -123,
                "date": 1_700_000_001,
                "text": "First digest text",
                "attachments": [
                    {
                        "type": "photo",
                        "photo": {"sizes": [{"url": f"https://img/{index}.jpg", "width": 10, "height": 10}]},
                    }
                    for index in range(1, 7)
                ],
            }
        ),
        monitor.build_daily_digest_post(
            {
                "id": 2,
                "owner_id": -123,
                "date": 1_700_000_101,
                "text": "Second digest text",
                "attachments": [
                    {
                        "type": "photo",
                        "photo": {"sizes": [{"url": f"https://img/{index}.jpg", "width": 10, "height": 10}]},
                    }
                    for index in range(7, 12)
                ],
            }
        ),
    ]
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 300}},
        status=200,
    )
    responses.add(
        responses.POST,
        tg_media_group_url(config),
        json={"ok": True, "result": []},
        status=200,
    )
    responses.add(
        responses.POST,
        tg_media_group_url(config),
        json={"ok": True, "result": []},
        status=200,
    )

    monitor.send_daily_digest_entries([entry for entry in entries if entry is not None], "https://vk.com/wall-123")

    send_calls = [call for call in responses.calls if "/sendMessage" in call.request.url]
    media_calls = [call for call in responses.calls if "/sendMediaGroup" in call.request.url]
    assert len(send_calls) == 1
    assert "https://vk.com/wall-123" in extract_message_text(send_calls[0])
    assert len(media_calls) == 2
    first_media = extract_media_payload(media_calls[0])
    second_media = extract_media_payload(media_calls[1])
    assert first_media[0]["media"] == "https://img/1.jpg"
    assert first_media[-1]["media"] == "https://img/10.jpg"
    assert all("caption" not in item for item in first_media)
    assert second_media[0]["media"] == "https://img/11.jpg"
    monitor.close()


@responses.activate
def test_send_telegram_photo_logs_failed_download_url(tmp_path: Path, caplog) -> None:
    config = make_config(tmp_path, enable_daily_digest=True)
    monitor = Monitor(config=config, sleeper=lambda _: None)
    responses.add(
        responses.GET,
        "https://img/fail.jpg",
        body="missing",
        status=404,
    )

    try:
        monitor.send_telegram_photo("https://img/fail.jpg", caption="x")
        assert False, "expected download failure"
    except Exception as exc:
        assert "https://img/fail.jpg" in str(exc)

    assert "https://img/fail.jpg" in caplog.text
    assert "Failed to download digest photo" in caplog.text
    monitor.close()


@responses.activate
def test_send_telegram_photo_logs_failed_upload_url(tmp_path: Path, caplog) -> None:
    config = make_config(tmp_path, enable_daily_digest=True)
    monitor = Monitor(config=config, sleeper=lambda _: None)
    responses.add(
        responses.GET,
        "https://img/upload.jpg",
        body=b"jpg-data",
        headers={"Content-Type": "image/jpeg"},
        status=200,
    )
    responses.add(
        responses.POST,
        tg_photo_url(config),
        json={"ok": False, "description": "bad"},
        status=400,
    )

    try:
        monitor.send_telegram_photo("https://img/upload.jpg")
        assert False, "expected upload failure"
    except Exception as exc:
        assert "Telegram HTTP 400" in str(exc)

    assert "https://img/upload.jpg" in caplog.text
    assert "Telegram sendPhoto failed" in caplog.text
    monitor.close()


@responses.activate
def test_send_telegram_media_group_logs_failed_batch_urls(tmp_path: Path, caplog) -> None:
    config = make_config(tmp_path, enable_daily_digest=True)
    monitor = Monitor(config=config, sleeper=lambda _: None)
    for suffix in ("a", "b"):
        responses.add(
            responses.GET,
            f"https://img/{suffix}.jpg",
            body=f"img-{suffix}".encode(),
            headers={"Content-Type": "image/jpeg"},
            status=200,
        )
    responses.add(
        responses.POST,
        tg_media_group_url(config),
        json={"ok": False, "description": "bad"},
        status=400,
    )

    try:
        monitor.send_telegram_media_group(["https://img/a.jpg", "https://img/b.jpg"])
        assert False, "expected media group upload failure"
    except Exception as exc:
        assert "Telegram HTTP 400" in str(exc)

    assert "https://img/a.jpg" in caplog.text
    assert "https://img/b.jpg" in caplog.text
    assert "Telegram sendMediaGroup failed" in caplog.text
    monitor.close()


@responses.activate
def test_digest_command_builds_current_day_without_persisting_state(tmp_path: Path) -> None:
    config = make_config(tmp_path, enable_daily_digest=True)
    state = StateStore(config.state_path)
    monitor = Monitor(config=config, state=state, sleeper=lambda _: None)
    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        json={"ok": True, "result": [{"update_id": 700, "message": {"chat": {"id": 123456}, "text": "/digest"}}]},
        status=200,
    )
    responses.add(
        responses.GET,
        VK_URL,
        json={
            "response": {
                "items": [
                    {
                        "id": 8,
                        "owner_id": -123,
                        "date": utc_ts(2026, 3, 16),
                        "text": "Today digest",
                    }
                ]
            }
        },
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 701}},
        status=200,
    )

    monitor.poll_telegram_updates_once()

    assert count_calls(responses.calls, "/sendMessage") == 1
    assert state.get_meta("last_daily_digest_date_sent") is None
    assert state.get_meta("last_tg_update_id") == "700"
    monitor.close()


@responses.activate
def test_run_processes_startup_daily_digest_catch_up(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        enable_instant_alerts=False,
        enable_daily_digest=True,
        daily_digest_time="08:00",
    )
    state = StateStore(config.state_path)
    state.set_last_seen(-123, 1)
    sleeps: list[float] = []

    def stop_after_first_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        raise KeyboardInterrupt

    monitor = Monitor(config=config, state=state, sleeper=stop_after_first_sleep)
    responses.add(
        responses.GET,
        VK_URL,
        json={"response": {"items": []}},
        status=200,
    )
    responses.add(
        responses.GET,
        tg_get_updates_url(config),
        json={"ok": True, "result": []},
        status=200,
    )
    responses.add(
        responses.GET,
        VK_URL,
        json={
            "response": {
                "items": [
                    {
                        "id": 5,
                        "owner_id": -123,
                        "date": utc_ts(2026, 3, 15),
                        "text": "Yesterday digest",
                    }
                ]
            }
        },
        status=200,
    )
    responses.add(
        responses.POST,
        tg_url(config),
        json={"ok": True, "result": {"message_id": 900}},
        status=200,
    )

    exit_code = command_run(monitor)

    assert exit_code == 0
    assert state.get_meta("last_daily_digest_date_sent") == (date(2026, 3, 16) - timedelta(days=1)).isoformat()
    assert sleeps
    monitor.close()
