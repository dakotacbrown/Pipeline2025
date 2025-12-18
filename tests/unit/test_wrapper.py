import importlib
import sys
import types
from dataclasses import dataclass

import pytest


@pytest.fixture()
def api_wrapper_module(monkeypatch):
    """
    Import src.api_wrapper with its external deps stubbed out in sys.modules.
    Returns the imported module object.
    """

    # --- stub: setup_logger ---
    class DummyLog:
        def __init__(self):
            self.infos = []
            self.warnings = []

        def info(self, msg, *args, **kwargs):
            self.infos.append((msg, args, kwargs))

        def warning(self, msg, *args, **kwargs):
            self.warnings.append((msg, args, kwargs))

    dummy_log = DummyLog()

    basic_logger_mod = types.ModuleType(
        "asvclscoredataservices_common.logger.basic_logger"
    )
    basic_logger_mod.setup_logger = lambda: dummy_log

    # --- stub: ApiIngester class ---
    @dataclass
    class DummyApiIngester:
        config: dict
        log: object

        def run_once(self, table_name: str, env_name: str):
            return {
                "mode": "once",
                "table_name": table_name,
                "env_name": env_name,
            }

        def run_backfill(self, table_name: str, env_name: str, start, end):
            return {
                "mode": "backfill",
                "table_name": table_name,
                "env_name": env_name,
                "start": str(start),
                "end": str(end),
            }

    ingester_mod = types.ModuleType(
        "asvclscoredataservices_common.ingester.api_ingester"
    )
    ingester_mod.ApiIngester = DummyApiIngester

    # Ensure the package parents also exist (Python import machinery expects them)
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common",
        types.ModuleType("asvclscoredataservices_common"),
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common.logger",
        types.ModuleType("asvclscoredataservices_common.logger"),
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common.logger.basic_logger",
        basic_logger_mod,
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common.ingester",
        types.ModuleType("asvclscoredataservices_common.ingester"),
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common.ingester.api_ingester",
        ingester_mod,
    )

    # Now import your module
    mod = importlib.import_module("src.api_wrapper")
    importlib.reload(mod)
    # expose dummy log for assertions
    mod._dummy_log = dummy_log
    return mod


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """
    Make env mutations safe per-test.
    """
    # start with empty env mapping layered on top of real os.environ
    monkeypatch.setenv("PYTEST_RUNNING", "1")
    yield


def test_set_env_vars_from_dict_sets_uppercase_and_skips_empty(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module

    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.delenv("BAR", raising=False)

    mod.set_env_vars_from_dict({"foo": "123", "": "nope", "bar": ""})

    assert mod.os.environ["FOO"] == "123"
    assert "BAR" not in mod.os.environ  # skipped because value empty

    # warning was logged for skipped entries
    assert any(
        "Skipping empty env var" in rec[0] for rec in mod._dummy_log.warnings
    )


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

    monkeypatch.setattr(mod.requests, "post", fake_post)

    token = mod.retrieve_oauth_token(
        oauth_link="https://oauth.example/token",
        headers={"h": "v"},
        data={"grant_type": "client_credentials"},
    )
    assert token == "abc123"


def test_run_ingester_sets_env_vars_and_runs_once(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module

    # stub oauth token retrieval
    monkeypatch.setattr(mod, "retrieve_oauth_token", lambda *a, **k: "cl_token")

    event = {
        "env_vars": {"dev": {"foo": "1"}, "prod": {"foo": "9"}},
        "cl_oauth_url": "https://cl/oauth",
        "exchange_headers": {"a": "b"},
        "exchange_data": {"x": "y"},
    }

    meta = mod.run_ingester(
        table="events_api",
        env="dev",
        event=event,
        config={"some": "yaml"},
        run_mode="once",
    )

    # env var payload chosen from env_vars[env]
    assert mod.os.environ["FOO"] == "1"
    assert mod.os.environ["CL_OAUTH_TOKEN"] == "cl_token"
    assert mod.os.environ["ENV"] == "dev"
    assert mod.os.environ["TABLE"] == "events_api"

    assert meta["mode"] == "once"
    assert meta["table_name"] == "events_api"
    assert meta["env_name"] == "dev"


def test_run_ingester_data_oauth_optional(api_wrapper_module, monkeypatch):
    mod = api_wrapper_module

    tokens = iter(["cl_token", "data_token"])
    monkeypatch.setattr(
        mod, "retrieve_oauth_token", lambda *a, **k: next(tokens)
    )

    event = {
        "cl_oauth_url": "https://cl/oauth",
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

    assert mod.os.environ["CL_OAUTH_TOKEN"] == "cl_token"
    assert mod.os.environ["DATA_OAUTH_TOKEN"] == "data_token"
    assert meta["mode"] == "once"


def test_run_ingester_backfill_requires_start_end(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module
    monkeypatch.setattr(mod, "retrieve_oauth_token", lambda *a, **k: "cl_token")

    event = {
        "cl_oauth_url": "https://cl/oauth",
        "exchange_headers": {},
        "exchange_data": {},
    }

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


def test_run_ingester_backfill_parses_dates_and_calls_backfill(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module
    monkeypatch.setattr(mod, "retrieve_oauth_token", lambda *a, **k: "cl_token")

    event = {
        "cl_oauth_url": "https://cl/oauth",
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

    assert meta["mode"] == "backfill"
    assert meta["start"] == "2025-01-01"
    assert meta["end"] == "2025-01-03"


def test_run_ingester_requires_table_and_env(api_wrapper_module, monkeypatch):
    mod = api_wrapper_module
    monkeypatch.setattr(mod, "retrieve_oauth_token", lambda *a, **k: "cl_token")

    event = {"cl_oauth_url": "x", "exchange_headers": {}, "exchange_data": {}}

    with pytest.raises(ValueError):
        mod.run_ingester(table="", env="dev", event=event, config={})
    with pytest.raises(ValueError):
        mod.run_ingester(table="t", env="", event=event, config={})
