import os
from datetime import date
from typing import Dict
from unittest.mock import MagicMock

import pytest

from utils import ingestor_wrapper

# ---------- helpers ----------


@pytest.fixture
def clean_environ():
    """Snapshot and restore os.environ so tests don't leak."""
    old = os.environ.copy()
    os.environ.clear()
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old)


@pytest.fixture
def base_config() -> Dict:
    return {
        "env_vars": {
            "dev": {"foo": "bar"},
            "prod": {"foo": "baz"},
        }
    }


@pytest.fixture
def base_event() -> Dict:
    return {
        "c1_oauth_url": "https://c1-token.example.com",
        "exchange_headers": {"ex": "hdr"},
        "exchange_data": {"ex": "data"},
        "data_auth_url": "https://data-token.example.com",
        "data_headers": {"dh": "val"},
        "data_auth": {"client_id": "id"},
    }


# ---------- set_env_vars_from_dict ----------


def test_set_env_vars_from_dict_sets_only_non_empty(monkeypatch, clean_environ):
    class DummyLog:
        def __init__(self):
            self.infos = []
            self.warnings = []

        def info(self, msg, *args):
            self.infos.append(msg % args if args else msg)

        def warning(self, msg, *args):
            self.warnings.append(msg % args if args else msg)

        def debug(self, *_, **__): ...
        def error(self, *_, **__): ...
        def exception(self, *_, **__): ...

    dummy_log = DummyLog()
    monkeypatch.setattr(ingestor_wrapper, "log", dummy_log)

    env_vars = {"foo": "bar", "empty": "", "none": None}

    ingestor_wrapper.set_env_vars_from_dict(env_vars)

    # non-empty key/value promoted to upper-case key
    assert os.environ["FOO"] == "bar"
    # empty / None not set
    assert "EMPTY" not in os.environ
    assert "NONE" not in os.environ

    # we logged about skipping empties
    assert any("Skipping empty env var" in m for m in dummy_log.warnings)
    assert any("Set env foo=bar" in m for m in dummy_log.infos)


# ---------- retrieve_oauth_token ----------


def test_retrieve_oauth_token_posts_and_returns_token(monkeypatch):
    fake_response = MagicMock()
    fake_response.json.return_value = {"access_token": "abc123"}
    fake_response.raise_for_status.return_value = None

    def fake_post(url, headers, data, verify):
        assert url == "https://token.example.com"
        assert headers == {"h": "v"}
        assert data == {"d": "v"}
        assert verify is False
        return fake_response

    monkeypatch.setattr(ingestor_wrapper.requests, "post", fake_post)

    token = ingestor_wrapper.retrieve_oauth_token(
        "https://token.example.com",
        {"h": "v"},
        {"d": "v"},
    )

    assert token == "abc123"
    fake_response.raise_for_status.assert_called_once()
    fake_response.json.assert_called_once()


# ---------- run_ingester (once mode) ----------


def test_run_ingester_once_sets_env_and_calls_run_once(
    monkeypatch, clean_environ, base_config, base_event
):
    # ApiIngester mock and its instance
    api_cls = MagicMock()
    api_instance = api_cls.return_value
    api_instance.run_once.return_value = {"rows": 7}

    monkeypatch.setattr(ingestor_wrapper, "ApiIngester", api_cls)

    # retrieve_oauth_token called twice: c1 and data
    monkeypatch.setattr(
        ingestor_wrapper,
        "retrieve_oauth_token",
        MagicMock(side_effect=["c1tok", "datatok"]),
    )

    meta = ingestor_wrapper.run_ingester(
        table="accounts",
        env="dev",
        event=base_event,
        config=base_config,
        run_mode="once",
        start=None,
        end=None,
    )

    # ApiIngester constructed correctly
    api_cls.assert_called_once_with(
        config=base_config, log=ingestor_wrapper.log
    )

    # run_once used, not backfill
    api_instance.run_once.assert_called_once_with(
        table_name="accounts",
        env_name="dev",
    )
    api_instance.run_backfill.assert_not_called()

    # return value is pass-through meta
    assert meta == {"rows": 7}

    # env_vars from config were applied
    assert os.environ["FOO"] == "bar"
    # oauth tokens set
    assert os.environ["C1_OAUTH_TOKEN"] == "c1tok"
    assert os.environ["DATA_OAUTH_TOKEN"] == "datatok"
    # basic job context
    assert os.environ["ENV"] == "dev"
    assert os.environ["TABLE"] == "accounts"
    # no dates in once mode
    assert "START_DATE" not in os.environ
    assert "END_DATE" not in os.environ


# ---------- run_ingester (backfill mode) ----------


def test_run_ingester_backfill_calls_run_backfill_with_dates(
    monkeypatch, clean_environ, base_config, base_event
):
    api_cls = MagicMock()
    api_instance = api_cls.return_value
    api_instance.run_backfill.return_value = {"rows": 99}

    monkeypatch.setattr(ingestor_wrapper, "ApiIngester", api_cls)
    monkeypatch.setattr(
        ingestor_wrapper,
        "retrieve_oauth_token",
        MagicMock(side_effect=["c1tok", "datatok"]),
    )

    meta = ingestor_wrapper.run_ingester(
        table="accounts",
        env="prod",
        event=base_event,
        config=base_config,
        run_mode="backfill",
        start="2024-01-01",
        end="2024-01-10",
    )

    api_instance.run_once.assert_not_called()
    api_instance.run_backfill.assert_called_once()
    _, kwargs = api_instance.run_backfill.call_args

    # kwargs use table_name/env_name and date objects
    assert kwargs["table_name"] == "accounts"
    assert kwargs["env_name"] == "prod"
    assert isinstance(kwargs["start"], date)
    assert isinstance(kwargs["end"], date)

    assert meta == {"rows": 99}
    assert os.environ["ENV"] == "prod"
    assert os.environ["TABLE"] == "accounts"
    assert os.environ["START_DATE"] == "2024-01-01"
    assert os.environ["END_DATE"] == "2024-01-10"


def test_run_ingester_backfill_requires_start_and_end(
    monkeypatch, clean_environ, base_config, base_event
):
    # ApiIngester shouldn't even be used when validation fails,
    # but patch it anyway to be safe.
    monkeypatch.setattr(ingestor_wrapper, "ApiIngester", MagicMock())
    monkeypatch.setattr(
        ingestor_wrapper,
        "retrieve_oauth_token",
        MagicMock(return_value="c1tok"),
    )

    # missing start
    with pytest.raises(ValueError):
        ingestor_wrapper.run_ingester(
            table="accounts",
            env="dev",
            event=base_event,
            config=base_config,
            run_mode="backfill",
            start=None,
            end="2024-01-10",
        )

    # missing end
    with pytest.raises(ValueError):
        ingestor_wrapper.run_ingester(
            table="accounts",
            env="dev",
            event=base_event,
            config=base_config,
            run_mode="backfill",
            start="2024-01-01",
            end=None,
        )


def test_run_ingester_backfill_invalid_date_format_raises(
    monkeypatch, clean_environ, base_config, base_event
):
    monkeypatch.setattr(ingestor_wrapper, "ApiIngester", MagicMock())
    monkeypatch.setattr(
        ingestor_wrapper,
        "retrieve_oauth_token",
        MagicMock(return_value="c1tok"),
    )

    with pytest.raises(ValueError):
        ingestor_wrapper.run_ingester(
            table="accounts",
            env="dev",
            event=base_event,
            config=base_config,
            run_mode="backfill",
            start="20240101",  # bad format
            end="2024-01-10",
        )


# ---------- run_ingester validation of table/env ----------


def test_run_ingester_requires_table_and_env(
    monkeypatch, clean_environ, base_config, base_event
):
    monkeypatch.setattr(ingestor_wrapper, "ApiIngester", MagicMock())
    monkeypatch.setattr(
        ingestor_wrapper,
        "retrieve_oauth_token",
        MagicMock(return_value="c1tok"),
    )

    # missing table
    with pytest.raises(ValueError):
        ingestor_wrapper.run_ingester(
            table="",
            env="dev",
            event=base_event,
            config=base_config,
        )

    # missing env
    with pytest.raises(ValueError):
        ingestor_wrapper.run_ingester(
            table="accounts",
            env="",
            event=base_event,
            config=base_config,
        )
