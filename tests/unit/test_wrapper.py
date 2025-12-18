import importlib
import os
import sys
import types
from dataclasses import dataclass

import pytest


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """
    Ensure env vars set in one test don't leak into another.
    """
    # Keep real env, but make sure our keys are clean.
    for k in [
        "PYTEST_FOO",
        "PYTEST_BAR",
        "C1_OAUTH_TOKEN",
        "DATA_OAUTH_TOKEN",
        "ENV",
        "TABLE",
        "START_DATE",
        "END_DATE",
    ]:
        monkeypatch.delenv(k, raising=False)
    yield
    for k in [
        "PYTEST_FOO",
        "PYTEST_BAR",
        "C1_OAUTH_TOKEN",
        "DATA_OAUTH_TOKEN",
        "ENV",
        "TABLE",
        "START_DATE",
        "END_DATE",
    ]:
        monkeypatch.delenv(k, raising=False)


@pytest.fixture()
def api_wrapper_module(monkeypatch):
    """
    Import src.api_wrapper with external deps stubbed in sys.modules.
    This prevents importing real asvclscoredataservices_common.* and prevents
    any accidental HTTP calls via ApiIngester or logger setup.
    """

    # ---- logger stub
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

    # ---- ApiIngester stub: prevents any real API calls
    @dataclass
    class DummyApiIngester:
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

    ingester_mod = types.ModuleType(
        "asvclscoredataservices_common.ingester.api_ingester"
    )
    ingester_mod.ApiIngester = DummyApiIngester

    # ---- install parent packages + modules
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

    # ---- import module under test AFTER stubs
    mod = importlib.import_module("src.api_wrapper")
    importlib.reload(mod)

    # expose dummy log for assertions
    mod._dummy_log = dummy_log
    return mod


def test_set_env_vars_from_dict_sets_uppercase_and_skips_empty(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module

    # ensure clean
    monkeypatch.delenv("PYTEST_FOO", raising=False)
    monkeypatch.delenv("PYTEST_BAR", raising=False)

    mod.set_env_vars_from_dict(
        {"pytest_foo": "123", "": "nope", "pytest_bar": ""}
    )

    assert os.environ["PYTEST_FOO"] == "123"
    assert "PYTEST_BAR" not in os.environ  # skipped because empty value

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

    # Patch requests.post in the module-under-test
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

    # Patch OAuth helper so no HTTP happens
    monkeypatch.setattr(mod, "retrieve_oauth_token", lambda *a, **k: "token123")

    event = {
        # NOTE: c1_oauth_url (not cl_oauth_url)
        "c1_oauth_url": "https://c1/oauth",
        "exchange_headers": {"a": "b"},
        "exchange_data": {"x": "y"},
        # env_vars payload supports either flat dict or nested-by-env
        "env_vars": {"dev": {"pytest_foo": "1"}, "prod": {"pytest_foo": "9"}},
    }

    meta = mod.run_ingester(
        table="events_api",
        env="dev",
        event=event,
        config={"some": "yaml"},
        run_mode="once",
    )

    # env vars from env_vars[env]
    assert os.environ["PYTEST_FOO"] == "1"

    # wrapper-set env vars
    assert os.environ["C1_OAUTH_TOKEN"] == "token123"
    assert os.environ["ENV"] == "dev"
    assert os.environ["TABLE"] == "events_api"

    assert meta["mode"] == "once"
    assert meta["table"] == "events_api"
    assert meta["env"] == "dev"


def test_run_ingester_data_oauth_optional(api_wrapper_module, monkeypatch):
    mod = api_wrapper_module

    tokens = iter(["c1_token", "data_token"])
    monkeypatch.setattr(
        mod, "retrieve_oauth_token", lambda *a, **k: next(tokens)
    )

    event = {
        "c1_oauth_url": "https://c1/oauth",
        "exchange_headers": {},
        "exchange_data": {},
        # optional data oauth
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


def test_run_ingester_backfill_requires_start_end(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module
    monkeypatch.setattr(mod, "retrieve_oauth_token", lambda *a, **k: "token123")

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


def test_run_ingester_backfill_parses_dates_and_calls_backfill(
    api_wrapper_module, monkeypatch
):
    mod = api_wrapper_module
    monkeypatch.setattr(mod, "retrieve_oauth_token", lambda *a, **k: "token123")

    event = {"c1_oauth_url": "x", "exchange_headers": {}, "exchange_data": {}}

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

    # env vars set when start/end provided
    assert os.environ["START_DATE"] == "2025-01-01"
    assert os.environ["END_DATE"] == "2025-01-03"


def test_run_ingester_requires_table_and_env(api_wrapper_module, monkeypatch):
    mod = api_wrapper_module
    monkeypatch.setattr(mod, "retrieve_oauth_token", lambda *a, **k: "token123")

    event = {"c1_oauth_url": "x", "exchange_headers": {}, "exchange_data": {}}

    with pytest.raises(ValueError):
        mod.run_ingester(table="", env="dev", event=event, config={})

    with pytest.raises(ValueError):
        mod.run_ingester(table="t", env="", event=event, config={})
