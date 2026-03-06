from __future__ import annotations

from datetime import datetime
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
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def tg_url(config: Config) -> str:
    return f"https://api.telegram.org/bot{config.tg_bot_token}/sendMessage"


def tg_get_updates_url(config: Config) -> str:
    return f"https://api.telegram.org/bot{config.tg_bot_token}/getUpdates"


def count_calls(calls, marker: str) -> int:
    return sum(1 for call in calls if marker in call.request.url)


def extract_message_text(call) -> str:
    body = call.request.body
    parsed = parse_qs(body.decode() if isinstance(body, bytes) else body)
    return parsed["text"][0]


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
