import os
import sys
from pathlib import Path
from typing import Any, Dict
from zipfile import ZipFile
from unittest.mock import MagicMock

import pytest

from src import api_wrapper


# ------------------------
# _zip_candidates_from_sys_path
# ------------------------
def test_zip_candidates_from_sys_path_finds_unique_zips(tmp_path, monkeypatch):
    zip1 = tmp_path / "bundle_a.zip"
    zip2 = tmp_path / "bundle_b.zip"
    zip1.touch()
    zip2.touch()

    monkeypatch.setattr(
        sys,
        "path",
        [
            "plain/path",
            str(zip1),
            str(zip1) + "/Python",
            str(zip2) + "/something",
        ],
    )

    cands = api_wrapper._zip_candidates_from_sys_path()
    assert set(cands) == {zip1.resolve(), zip2.resolve()}


def test_zip_candidates_ignores_nonexistent_and_bad_entries(tmp_path, monkeypatch):
    zip1 = tmp_path / "exists.zip"
    zip1.touch()

    monkeypatch.setattr(
        sys,
        "path",
        [
            str(zip1),
            str(tmp_path / "nonexistent.zip"),
            "this_is_not_a_zip",
        ],
    )

    cands = api_wrapper._zip_candidates_from_sys_path()
    assert cands == [zip1.resolve()]


# ------------------------
# set_env_vars_from_dict
# ------------------------
def test_set_env_vars_from_dict_sets_uppercase():
    old_environ = os.environ.copy()
    try:
        api_wrapper.set_env_vars_from_dict(
            {
                "foo": "bar",
                "": "nope",
                "baz": "",
            }
        )

        assert os.environ["FOO"] == "bar"
        assert "BAZ" not in os.environ
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


# ------------------------
# retrieve_oauth_token
# ------------------------
def test_retrieve_oauth_token(monkeypatch):
    dummy_resp = MagicMock()
    dummy_resp.raise_for_status.return_value = None
    dummy_resp.json.return_value = {"access_token": "sekret"}

    post_mock = MagicMock(return_value=dummy_resp)
    monkeypatch.setattr(api_wrapper.requests, "post", post_mock)

    token = api_wrapper.retrieve_oauth_token(
        "https://token.example.com",
        headers={"x": "y"},
        data={"a": "b"},
    )

    assert token == "sekret"
    post_mock.assert_called_once_with(
        "https://token.example.com",
        headers={"x": "y"},
        data={"a": "b"},
        verify=False,
    )


# ------------------------
# _read_text_from_zip
# ------------------------
def test_read_text_from_zip_reads_first_matching_variant(tmp_path):
    zip_path = tmp_path / "bundle.zip"
    inner = "config/ingester.yml"

    with ZipFile(zip_path, "w") as zf:
        zf.writestr(inner, "answer: 42")

    text = api_wrapper._read_text_from_zip(zip_path, inner)
    assert text == "answer: 42"

    text2 = api_wrapper._read_text_from_zip(zip_path, "/" + inner)
    assert text2 == "answer: 42"


def test_read_text_from_zip_returns_none_when_missing(tmp_path):
    zip_path = tmp_path / "bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        zf.writestr("other/file.txt", "nope")

    assert api_wrapper._read_text_from_zip(zip_path, "config/ingester.yml") is None


# ------------------------
# _load_yaml_from_anywhere
# ------------------------
def test_load_yaml_from_anywhere_from_filesystem(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    yaml_path = tmp_path / "config.yml"
    yaml_path.write_text("foo: 123\nbar: baz\n")

    cfg = api_wrapper._load_yaml_from_anywhere(str(yaml_path))

    assert cfg == {"foo": 123, "bar": "baz"}


def test_load_yaml_from_anywhere_from_zip(tmp_path, monkeypatch):
    yaml_rel = "config/ingester.yml"

    zip_path = tmp_path / "bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        zf.writestr(yaml_rel, "foo: 10\n")

    monkeypatch.setattr(
        api_wrapper,
        "_zip_candidates_from_sys_path",
        lambda: [zip_path],
    )

    cfg = api_wrapper._load_yaml_from_anywhere(yaml_rel)
    assert cfg == {"foo": 10}


def test_load_yaml_from_anywhere_raises_when_not_found(monkeypatch):
    monkeypatch.setattr(api_wrapper, "_zip_candidates_from_sys_path", lambda: [])

    with pytest.raises(FileNotFoundError):
        api_wrapper._load_yaml_from_anywhere("missing.yml")


# ------------------------
# helpers to patch ApiIngester with MagicMock
# ------------------------
def _mock_api_ingester(monkeypatch) -> MagicMock:
    """
    Replace ApiIngester in api_wrapper with a MagicMock that returns
    an instance mock. Returns the instance mock so tests can set
    run_once / run_backfill return values and assertions.
    """
    instance = MagicMock()
    cls_mock = MagicMock(return_value=instance)
    monkeypatch.setattr(api_wrapper, "ApiIngester", cls_mock)
    return instance


# ------------------------
# run_ingester tests
# ------------------------
def test_run_ingester_once_sets_env_and_calls_run_once(monkeypatch):
    monkeypatch.setattr(
        api_wrapper,
        "_load_yaml_from_anywhere",
        lambda path: {"apis": {"foo": "bar"}},
    )

    env_vars_seen: Dict[str, str] = {}

    def fake_set_env_vars(env_vars):
        env_vars_seen.update(env_vars)

    monkeypatch.setattr(api_wrapper, "set_env_vars_from_dict", fake_set_env_vars)

    monkeypatch.setattr(
        api_wrapper,
        "retrieve_oauth_token",
        MagicMock(return_value="tok123"),
    )

    ingester_instance = _mock_api_ingester(monkeypatch)
    ingester_instance.run_once.return_value = {"rows": 5}

    old_environ = os.environ.copy()
    try:
        event = {
            "env_vars": {"foo": "bar"},
            "c1_oauth_url": "https://c1-token.example.com",
            "exchange_headers": {"ex": "hdr"},
            "exchange_data": {"ex": "data"},
            "data_auth": {"c1_oauth_url": "https://c1-token.example.com"},
            "data_headers": {"hdr": "val"},
        }

        meta = api_wrapper.run_ingester(
            table="accounts",
            env_name="dev",
            yaml_path="config/ingester.yml",
            event=event,
            run_mode="once",
            start=None,
            end=None,
        )

        assert meta == {"rows": 5}
        assert env_vars_seen == {"foo": "bar"}
        assert os.environ["ENV"] == "dev"
        assert os.environ["TABLE"] == "accounts"
        assert os.environ["DATA_AUTH_TOKEN"] == "tok123"

        ingester_instance.run_once.assert_called_once_with(
            table_name="accounts",
            env_name="dev",
        )
        ingester_instance.run_backfill.assert_not_called()
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def test_run_ingester_backfill_calls_run_backfill(monkeypatch):
    monkeypatch.setattr(
        api_wrapper,
        "_load_yaml_from_anywhere",
        lambda path: {"apis": {}},
    )
    monkeypatch.setattr(api_wrapper, "set_env_vars_from_dict", lambda env: None)
    monkeypatch.setattr(
        api_wrapper,
        "retrieve_oauth_token",
        MagicMock(return_value="tok"),
    )

    ingester_instance = _mock_api_ingester(monkeypatch)
    ingester_instance.run_backfill.return_value = {"mode": "backfill"}

    old_environ = os.environ.copy()
    try:
        meta = api_wrapper.run_ingester(
            table="events",
            env_name="prod",
            yaml_path="config.yml",
            event={"c1_oauth_url": "https://c1-token.example.com"},
            run_mode="backfill",
            start="2024-01-01",
            end="2024-01-10",
        )

        assert meta == {"mode": "backfill"}
        ingester_instance.run_backfill.assert_called_once()
        ingester_instance.run_once.assert_not_called()
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def test_run_ingester_backfill_requires_start_and_end(monkeypatch):
    monkeypatch.setattr(api_wrapper, "_load_yaml_from_anywhere", lambda p: {})
    monkeypatch.setattr(api_wrapper, "set_env_vars_from_dict", lambda env: None)
    monkeypatch.setattr(
        api_wrapper,
        "retrieve_oauth_token",
        MagicMock(return_value="tok"),
    )
    _mock_api_ingester(monkeypatch)

    with pytest.raises(ValueError):
        api_wrapper.run_ingester(
            table="t",
            env_name="dev",
            yaml_path="cfg.yml",
            event={"c1_oauth_url": "https://c1-token.example.com"},
            run_mode="backfill",
            start="2024-01-01",
            end=None,
        )
