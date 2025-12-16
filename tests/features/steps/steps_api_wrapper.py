import json
import os
from unittest.mock import MagicMock, patch

from behave import given, then, when


@given("a valid wrapper config and event")
def step_valid_wrapper_config_and_event(context):
    # Avoid context.config (you were getting KeyError / collisions). Use distinct names.
    context.wrapper_config = {"dummy": "config"}  # wrapper just passes this through to ApiIngester
    context.table_name = "events_api"
    context.env_name = "dev"

    context.wrapper_event = {
        # IMPORTANT: wrapper does env_vars[env] so env must exist here
        "env_vars": {
            "dev": {"FOO": "bar"},
        },
        "exchange_headers": {"Content-Type": "application/x-www-form-urlencoded"},
        "exchange_data": {"grant_type": "client_credentials"},
        "c1_oauth_url": "https://example.com/c1/oauth",
        # optional data oauth branch
        "data_headers": {"Content-Type": "application/x-www-form-urlencoded"},
        "data_auth": {"grant_type": "client_credentials"},
        "data_oauth_url": "https://example.com/data/oauth",
    }


@when('run_ingester is called in "once" mode')
def step_run_ingester_once(context):
    # Patch ApiIngester in *this module path*
    with patch("src.api_wrapper.ApiIngester") as MockIngester, patch(
        "src.api_wrapper.requests.post"
    ) as mock_post:
        # oauth response mocks (called twice: c1 + data)
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.side_effect = [{"access_token": "C1_TOKEN"}, {"access_token": "DATA_TOKEN"}]
        mock_post.return_value = resp

        instance = MockIngester.return_value
        instance.run_once.return_value = {"rows": 123}

        from src.api_wrapper import run_ingester

        context._mock_ingester = instance
        context.result = run_ingester(
            table=context.table_name,
            env=context.env_name,
            event=context.wrapper_event,
            config=context.wrapper_config,
            run_mode="once",
        )


@then("ApiIngester.run_once is invoked")
def step_assert_run_once(context):
    context._mock_ingester.run_once.assert_called_once_with(
        table=context.table_name, env_name=context.env_name
    )


@then("the wrapper returns metadata")
def step_assert_returns_meta(context):
    assert context.result == {"rows": 123}
    # sanity check it set env (from screenshots)
    assert os.environ["ENV"] == "dev"
    assert os.environ["TABLE"] == "events_api"
    assert os.environ["C1_OAUTH_TOKEN"] == "C1_TOKEN"
    assert os.environ["DATA_OAUTH_TOKEN"] == "DATA_TOKEN"


@when('run_ingester is called in "backfill" mode with dates')
def step_run_ingester_backfill(context):
    with patch("src.api_wrapper.ApiIngester") as MockIngester, patch(
        "src.api_wrapper.requests.post"
    ) as mock_post:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.side_effect = [{"access_token": "C1_TOKEN"}, {"access_token": "DATA_TOKEN"}]
        mock_post.return_value = resp

        instance = MockIngester.return_value
        instance.run_backfill.return_value = {"rows": 999}

        from src.api_wrapper import run_ingester

        context._mock_ingester = instance
        context.result = run_ingester(
            table=context.table_name,
            env=context.env_name,
            event=context.wrapper_event,
            config=context.wrapper_config,
            run_mode="backfill",
            start="2025-01-01",
            end="2025-01-02",
        )


@then("ApiIngester.run_backfill is invoked")
def step_assert_run_backfill(context):
    # We don't hard-assert date objects here; just confirm it called with correct table/env.
    args, kwargs = context._mock_ingester.run_backfill.call_args
    assert kwargs["table"] == context.table_name
    assert kwargs["env_name"] == context.env_name
    assert "start" in kwargs and "end" in kwargs


@when('run_ingester is called in "backfill" mode without dates')
def step_run_ingester_backfill_no_dates(context):
    from src.api_wrapper import run_ingester

    context.raised = None
    try:
        run_ingester(
            table=context.table_name,
            env=context.env_name,
            event=context.wrapper_event,
            config=context.wrapper_config,
            run_mode="backfill",
            start=None,
            end=None,
        )
    except Exception as e:
        context.raised = e


@then("a ValueError is raised")
def step_assert_value_error(context):
    assert isinstance(context.raised, ValueError), f"Expected ValueError, got: {context.raised!r}"
