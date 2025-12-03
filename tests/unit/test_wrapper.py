import os
import sys
from pathlib import Path
from typing import Any, Dict
from zipfile import ZipFile

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
            str(zip1) + "/Python",   # duplicate reference
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

        # valid key/value set, and uppercased
        assert os.environ["FOO"] == "bar"

        # invalid entries skipped
        assert "BAZ" not in os.environ
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


# ------------------------
# retrieve_oauth_token
# ------------------------
def test_retrieve_oauth_token(monkeypatch):
    class DummyResponse:
        def __init__(self):
            self._json = {"access_token": "sekret"}

        def raise_for_status(self):
            pass

        def json(self):
            return self._json

    calls: Dict[str, Any] = {}

    def fake_post(url, headers=None, data=None, verify=None):
        calls["url"] = url
        calls["headers"] = headers
        calls["data"] = data
        calls["verify"] = verify
        return DummyResponse()

    monkeypatch.setattr(api_wrapper.requests, "post", fake_post)

    token = api_wrapper.retrieve_oauth_token(
        "https://token.example.com",
        headers={"x": "y"},
        data={"a": "b"},
    )

    assert token == "sekret"
    assert calls["url"] == "https://token.example.com"
    assert calls["headers"] == {"x": "y"}
    assert calls["data"] == {"a": "b"}


# ------------------------
# _read_text_from_zip
# ------------------------
def test_read_text_from_zip_reads_first_matching_variant(tmp_path):
    zip_path = tmp_path / "bundle.zip"
    inner = "config/ingester.yml"

    with ZipFile(zip_path, "w") as zf:
        zf.writestr(inner, "answer: 42")

    # Should read raw path
    text = api_wrapper._read_text_from_zip(zip_path, inner)
    assert text == "answer: 42"

    # Should also work with leading slash
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

    # create zip with YAML inside
    zip_path = tmp_path / "bundle.zip"
    with ZipFile(zip_path, "w") as zf:
        zf.writestr(yaml_rel, "foo: 10\n")

    # force candidate list to our zip only
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
# run_ingester
# ------------------------
def _patch_api_ingester_to_dummy(monkeypatch, DummyIngester):
    """
    Patch both the alias in src.api_wrapper and the original
    library path so no real HTTP/database work happens.
    """
    monkeypatch.setattr(api_wrapper, "ApiIngester", DummyIngester)
    monkeypatch.setattr(
        "asvclscoresdataservices_common.ingester.api_ingester.ApiIngester",
        DummyIngester,
        raising=False,
    )


def test_run_ingester_once_sets_env_and_calls_run_once(monkeypatch):
    # stub YAML loader
    loaded_cfg: Dict[str, Any] = {"apis": {"foo": "bar"}}
    monkeypatch.setattr(
        api_wrapper,
        "_load_yaml_from_anywhere",
        lambda path: loaded_cfg,
    )

    # capture env_vars passed in
    env_vars_seen: Dict[str, str] = {}

    def fake_set_env_vars(env_vars):
        env_vars_seen.update(env_vars)

    monkeypatch.setattr(api_wrapper, "set_env_vars_from_dict", fake_set_env_vars)

    # stub OAuth: just return a token; we'll be called at least once
    calls_retrieve = {"count": 0}

    def fake_retrieve(*args, **kwargs):
        calls_retrieve["count"] += 1
        return "tok123"

    monkeypatch.setattr(api_wrapper, "retrieve_oauth_token", fake_retrieve)

    # stub ApiIngester
    calls_ingester: Dict[str, Any] = {}

    class DummyIngester:
        def __init__(self, *args, **kwargs):
            calls_ingester["init"] = (args, kwargs)

        def run_once(self, table, env_name):
            calls_ingester["run_once"] = (table, env_name)
            return {"rows": 5}

        def run_backfill(self, *args, **kwargs):
            calls_ingester["run_backfill"] = (args, kwargs)
            return {"rows": 99}

    _patch_api_ingester_to_dummy(monkeypatch, DummyIngester)

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

        # meta from DummyIngester.run_once
        assert meta == {"rows": 5}

        # env vars from dict were passed through
        assert env_vars_seen == {"foo": "bar"}

        # core envs set
        assert os.environ["ENV"] == "dev"
        assert os.environ["TABLE"] == "accounts"

        # oauth token was written (from data_auth)
        assert os.environ["DATA_AUTH_TOKEN"] == "tok123"

        # correct ingester method called
        assert calls_ingester["run_once"] == ("accounts", "dev")
        assert "run_backfill" not in calls_ingester
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
    monkeypatch.setattr(api_wrapper, "retrieve_oauth_token", lambda *a, **k: "tok")

    calls: Dict[str, Any] = {}

    class DummyIngester:
        def __init__(self, *args, **kwargs):
            pass

        def run_once(self, *a, **k):
            calls["run_once"] = True
            return {}

        def run_backfill(self, *args, **kwargs):
            calls["run_backfill"] = (args, kwargs)
            return {"mode": "backfill"}

    _patch_api_ingester_to_dummy(monkeypatch, DummyIngester)

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
        assert "run_backfill" in calls
        assert "run_once" not in calls
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def test_run_ingester_backfill_requires_start_and_end(monkeypatch):
    monkeypatch.setattr(api_wrapper, "_load_yaml_from_anywhere", lambda p: {})
    monkeypatch.setattr(api_wrapper, "set_env_vars_from_dict", lambda env: None)

    class DummyIngester:
        def __init__(self, *a, **k):
            pass

    _patch_api_ingester_to_dummy(monkeypatch, DummyIngester)

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
