# tests/features/steps/step_api_wrapper.py

import json
from datetime import date
from typing import Dict, Any
from unittest.mock import MagicMock

from behave import given, when, then

from src import api_wrapper


class DummyLogger:
    def debug(self, *a, **k): ...
    def info(self, *a, **k): ...
    def warning(self, *a, **k): ...
    def error(self, *a, **k): ...
    def exception(self, *a, **k): ...


@given("a basic API config and event")
def step_basic_api_config_and_event(context):
    # minimal config with env-specific env_vars
    context.config: Dict[str, Any] = {
        "env_vars": {
            "dev": {"foo": "bar"},
            "prod": {"foo": "baz"},
        }
    }

    # event that has both c1 and data auth
    context.event: Dict[str, Any] = {
        "c1_oauth_url": "https://c1-token.example.com",
        "exchange_headers": {"ex": "hdr"},
        "exchange_data": {"ex": "data"},
        "data_auth_url": "https://data-token.example.com",
        "data_headers": {"dh": "val"},
        "data_auth": {"client_id": "id"},
    }

    # patch logger so we don't care about real logging
    context.old_log = api_wrapper.log
    api_wrapper.log = DummyLogger()


@when('I call run_ingester in "once" mode')
def step_call_run_ingester_once(context):
    api_cls = MagicMock()
    api_instance = api_cls.return_value
    api_instance.run_once.return_value = {"rows": 10}  # predictable meta

    context.ApiIngester_mock = api_cls
    context.retrieve_oauth_token_mock = MagicMock(
        side_effect=["c1tok", "datatok"]
    )

    # patch ApiIngester and retrieve_oauth_token
    context._old_ApiIngester = api_wrapper.ApiIngester
    context._old_retrieve = api_wrapper.retrieve_oauth_token
    api_wrapper.ApiIngester = api_cls
    api_wrapper.retrieve_oauth_token = context.retrieve_oauth_token_mock

    context.meta = api_wrapper.run_ingester(
        table="accounts",
        env="dev",
        event=context.event,
        config=context.config,
        run_mode="once",
        start=None,
        end=None,
    )


@then("ApiIngester.run_once is called and run_ingester returns the meta rows")
def step_assert_once_call(context):
    api_cls = context.ApiIngester_mock
    api_instance = api_cls.return_value

    api_cls.assert_called_once_with(config=context.config, log=api_wrapper.log)
    api_instance.run_once.assert_called_once_with(
        table_name="accounts",
        env_name="dev",
    )
    api_instance.run_backfill.assert_not_called()

    assert context.meta == {"rows": 10}

    # restore patched things
    api_wrapper.ApiIngester = context._old_ApiIngester
    api_wrapper.retrieve_oauth_token = context._old_retrieve
    api_wrapper.log = context.old_log


@when('I call run_ingester in "backfill" mode')
def step_call_run_ingester_backfill(context):
    api_cls = MagicMock()
    api_instance = api_cls.return_value
    api_instance.run_backfill.return_value = {"rows": 99}

    context.ApiIngester_mock = api_cls
    context.retrieve_oauth_token_mock = MagicMock(
        side_effect=["c1tok", "datatok"]
    )

    context._old_ApiIngester = api_wrapper.ApiIngester
    context._old_retrieve = api_wrapper.retrieve_oauth_token
    api_wrapper.ApiIngester = api_cls
    api_wrapper.retrieve_oauth_token = context.retrieve_oauth_token_mock

    context.meta = api_wrapper.run_ingester(
        table="accounts",
        env="dev",
        event=context.event,
        config=context.config,
        run_mode="backfill",
        start="2024-01-01",
        end="2024-01-10",
    )


@then("ApiIngester.run_backfill is called with date objects")
def step_assert_backfill_call(context):
    api_cls = context.ApiIngester_mock
    api_instance = api_cls.return_value

    api_instance.run_once.assert_not_called()
    api_instance.run_backfill.assert_called_once()
    _, kwargs = api_instance.run_backfill.call_args

    assert kwargs["table_name"] == "accounts"
    assert kwargs["env_name"] == "dev"
    assert isinstance(kwargs["start"], date)
    assert isinstance(kwargs["end"], date)

    assert context.meta == {"rows": 99}

    api_wrapper.ApiIngester = context._old_ApiIngester
    api_wrapper.retrieve_oauth_token = context._old_retrieve
    api_wrapper.log = context.old_log


@when("I call run_ingester in backfill mode without start/end")
def step_call_run_ingester_backfill_missing_dates(context):
    # no need to patch ApiIngester; we are testing validation
    context._error = None
    try:
        api_wrapper.run_ingester(
            table="accounts",
            env="dev",
            event=context.event,
            config=context.config,
            run_mode="backfill",
            start=None,
            end=None,
        )
    except Exception as exc:  # noqa: BLE001
        context._error = exc


@then("run_ingester raises a ValueError")
def step_assert_backfill_value_error(context):
    assert isinstance(context._error, ValueError)
    api_wrapper.log = context.old_log
