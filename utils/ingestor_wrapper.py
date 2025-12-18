import os
from types import SimpleNamespace

import pytest
from src import api_wrapper


def test_set_env_vars_from_dict_sets_uppercase(monkeypatch):
    monkeypatch.delenv("FOO", raising=False)

    api_wrapper.set_env_vars_from_dict({"foo": "bar"})
    assert os.environ["FOO"] == "bar"


def test_retrieve_oauth_token_posts_and_returns(monkeypatch):
    class DummyResp:
        def raise_for_status(self): ...
        def json(self):
            return {"access_token": "TOKEN123"}

    calls = {}

    def fake_post(url, headers=None, data=None, verify=None):
        calls["url"] = url
        calls["headers"] = headers
        calls["data"] = data
        calls["verify"] = verify
        return DummyResp()

    monkeypatch.setattr(api_wrapper.requests, "post", fake_post)

    tok = api_wrapper.retrieve_oauth_token(
        "https://oauth", {"h": "v"}, {"a": "b"}
    )
    assert tok == "TOKEN123"
    assert calls["verify"] is False


def test_run_ingester_once_sets_env_and_calls_ingester(monkeypatch):
    # Fake ApiIngester
    class DummyIngester:
        def __init__(self, config, log):
            self.config = config
            self.log = log

        def run_once(self, table_name, env_name):
            return {"mode": "once", "table": table_name, "env": env_name}

    monkeypatch.setattr(api_wrapper, "ApiIngester", DummyIngester)

    # Make oauth deterministic
    monkeypatch.setattr(
        api_wrapper, "retrieve_oauth_token", lambda *a, **k: "CLTOK"
    )

    event = {
        "env_vars": {"dev": {"x": "y"}},
        "c1_oauth_url": "https://oauth",
        "exchange_headers": {"Content-Type": "x"},
        "exchange_data": {"grant_type": "client_credentials"},
    }

    meta = api_wrapper.run_ingester(
        table="accounts",
        env="dev",
        event=event,
        config={"cfg": 1},
        run_mode="once",
        start=None,
        end=None,
    )

    assert meta["mode"] == "once"
    assert os.environ["ENV"] == "dev"
    assert os.environ["TABLE"] == "accounts"
    assert os.environ["CL_OAUTH_TOKEN"] == "CLTOK"
    assert os.environ["X"] == "y"  # from env_vars (uppercased)


def test_run_ingester_backfill_requires_dates(monkeypatch):
    class DummyIngester:
        def __init__(self, config, log): ...
        def run_backfill(self, table_name, env_name, start, end):
            return {"mode": "backfill"}

    monkeypatch.setattr(api_wrapper, "ApiIngester", DummyIngester)
    monkeypatch.setattr(
        api_wrapper, "retrieve_oauth_token", lambda *a, **k: "CLTOK"
    )

    event = {
        "env_vars": {"dev": {}},
        "c1_oauth_url": "https://oauth",
        "exchange_headers": {},
        "exchange_data": {},
    }

    with pytest.raises(ValueError, match="Backfill requires both"):
        api_wrapper.run_ingester(
            table="accounts",
            env="dev",
            event=event,
            config={},
            run_mode="backfill",
            start=None,
            end=None,
        )

    with pytest.raises(ValueError, match="Invalid start/end"):
        api_wrapper.run_ingester(
            table="accounts",
            env="dev",
            event=event,
            config={},
            run_mode="backfill",
            start="2025/01/01",
            end="2025-01-31",
        )
