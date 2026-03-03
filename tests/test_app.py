from __future__ import annotations

from pathlib import Path
from urllib.parse import parse_qs

import responses

from vk_wall_monitor.app import Config, Monitor, StateStore


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
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def tg_url(config: Config) -> str:
    return f"https://api.telegram.org/bot{config.tg_bot_token}/sendMessage"


def count_calls(calls, marker: str) -> int:
    return sum(1 for call in calls if marker in call.request.url)


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
