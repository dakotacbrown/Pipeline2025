import importlib
import os
from dataclasses import dataclass

import pytest


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """
    Prevent environment variables from leaking across tests.
    """
    keys = [
        "PYTEST_FOO",
        "PYTEST_BAR",
        "C1_OAUTH_TOKEN",
        "DATA_OAUTH_TOKEN",
        "ENV",
        "TABLE",
        "START_DATE",
        "END_DATE",
    ]
    for k in keys:
        monkeypatch.delenv(k, raising=False)
    yield
    for k in keys:
        monkeypatch.delenv(k, raising=False)


@pytest.fixture()
def api_wrapper_module():
    """
    Import/reload the wrapper module for each test file run.
    """
    mod = importlib.import_module("src.api_wrapper")
    importlib.reload(mod)
    return mod


@dataclass
class DummyApiIngester:
    """
    Fake ingester used for unit tests (no network, no config validation).
    """

    config: dict
    log: object

    def run_once(self, table_name: str, env_name: str):
        return {"mode": "once", "table": table_name, "env": env_name}

    def run_backfill(self, table_name: str, env_name: str, start, end):
        return {
            "mode": "backfill",
            "table": table_name,
            "env": env_name,
            "start": str(start),
            "end": str(end),
        }


def test_set_env_vars_from_dict_sets_uppercase_and_skips_empty(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module

    monkeypatch.delenv("PYTEST_FOO", raising=False)
    monkeypatch.delenv("PYTEST_BAR", raising=False)

    mod.set_env_vars_from_dict(
        {"pytest_foo": "123", "": "nope", "pytest_bar": ""}
    )

    assert os.environ["PYTEST_FOO"] == "123"
    assert "PYTEST_BAR" not in os.environ


def test_retrieve_oauth_token_happy_path(api_wrapper_module, monkeypatch):
    mod = api_wrapper_module

    class DummyResp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"access_token": "abc123"}

    def fake_post(url, headers=None, data=None, verify=None):
        assert url == "https://oauth.example/token"
        assert headers == {"h": "v"}
        assert data == {"grant_type": "client_credentials"}
        assert verify is False
        return DummyResp()

    # Patch requests.post at the module-under-test
    monkeypatch.setattr(mod.requests, "post", fake_post)

    token = mod.retrieve_oauth_token(
        oauth_link="https://oauth.example/token",
        headers={"h": "v"},
        data={"grant_type": "client_credentials"},
    )
    assert token == "abc123"


def test_run_ingester_runs_once_and_patches_ingester_class(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module

    # ✅ Patch the class used by the wrapper
    monkeypatch.setattr(mod, "ApiIngester", DummyApiIngester, raising=True)

    # ✅ Patch OAuth helper so no HTTP occurs
    monkeypatch.setattr(
        mod, "retrieve_oauth_token", lambda *a, **k: "token123", raising=True
    )

    event = {
        "c1_oauth_url": "https://c1/oauth",
        "exchange_headers": {"a": "b"},
        "exchange_data": {"x": "y"},
        "env_vars": {"dev": {"pytest_foo": "1"}},
    }

    meta = mod.run_ingester(
        table="events_api",
        env="dev",
        event=event,
        config={"some": "yaml"},
        run_mode="once",
    )

    assert os.environ["PYTEST_FOO"] == "1"
    assert os.environ["C1_OAUTH_TOKEN"] == "token123"
    assert os.environ["ENV"] == "dev"
    assert os.environ["TABLE"] == "events_api"

    assert meta == {"mode": "once", "table": "events_api", "env": "dev"}


def test_run_ingester_runs_backfill_and_sets_dates(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module

    monkeypatch.setattr(mod, "ApiIngester", DummyApiIngester, raising=True)
    monkeypatch.setattr(
        mod, "retrieve_oauth_token", lambda *a, **k: "token123", raising=True
    )

    event = {
        "c1_oauth_url": "https://c1/oauth",
        "exchange_headers": {},
        "exchange_data": {},
    }

    meta = mod.run_ingester(
        table="t",
        env="dev",
        event=event,
        config={},
        run_mode="backfill",
        start="2025-01-01",
        end="2025-01-03",
    )

    assert os.environ["START_DATE"] == "2025-01-01"
    assert os.environ["END_DATE"] == "2025-01-03"
    assert meta["mode"] == "backfill"
    assert meta["start"] == "2025-01-01"
    assert meta["end"] == "2025-01-03"


def test_run_ingester_backfill_requires_start_end(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module

    monkeypatch.setattr(mod, "ApiIngester", DummyApiIngester, raising=True)
    monkeypatch.setattr(
        mod, "retrieve_oauth_token", lambda *a, **k: "token123", raising=True
    )

    event = {"c1_oauth_url": "x", "exchange_headers": {}, "exchange_data": {}}

    with pytest.raises(ValueError):
        mod.run_ingester(
            table="t",
            env="dev",
            event=event,
            config={},
            run_mode="backfill",
            start=None,
            end=None,
        )


def test_run_ingester_requires_table_and_env(api_wrapper_module, monkeypatch):
    mod = api_wrapper_module

    monkeypatch.setattr(mod, "ApiIngester", DummyApiIngester, raising=True)
    monkeypatch.setattr(
        mod, "retrieve_oauth_token", lambda *a, **k: "token123", raising=True
    )

    event = {"c1_oauth_url": "x", "exchange_headers": {}, "exchange_data": {}}

    with pytest.raises(ValueError):
        mod.run_ingester(table="", env="dev", event=event, config={})

    with pytest.raises(ValueError):
        mod.run_ingester(table="t", env="", event=event, config={})


def test_run_ingester_optional_data_oauth_sets_data_token(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module

    monkeypatch.setattr(mod, "ApiIngester", DummyApiIngester, raising=True)

    # First call returns c1 token, second call returns data token
    tokens = iter(["c1_token", "data_token"])
    monkeypatch.setattr(
        mod, "retrieve_oauth_token", lambda *a, **k: next(tokens), raising=True
    )

    event = {
        "c1_oauth_url": "https://c1/oauth",
        "exchange_headers": {},
        "exchange_data": {},
        "data_headers": {"h": "v"},
        "data_auth": {"grant": "x"},
        "data_auth_url": "https://data/oauth",
    }

    meta = mod.run_ingester(
        table="t",
        env="dev",
        event=event,
        config={},
        run_mode="once",
    )

    assert os.environ["C1_OAUTH_TOKEN"] == "c1_token"
    assert os.environ["DATA_OAUTH_TOKEN"] == "data_token"
    assert meta["mode"] == "once"
