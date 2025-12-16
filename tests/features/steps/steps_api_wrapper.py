import os
from unittest.mock import MagicMock, patch

from behave import given, then, when


@given("a basic API config and event")
def step_basic_api_config_and_event(context):
    # Avoid context.config (Behave has its own config object)
    context.wrapper_config = {"some": "yaml-ish-config"}

    context.table = "events_api"
    context.env = "dev"

    # IMPORTANT: your wrapper does env_vars[env], so it MUST be nested by env key
    context.event = {
        "env_vars": {
            "dev": {"FOO": "bar"},
        },
        "exchange_headers": {"Content-Type": "application/x-www-form-urlencoded"},
        "exchange_data": {"grant_type": "client_credentials"},
        "c1_oauth_url": "https://example.com/oauth",
        # Only include these if your wrapper checks for them
        "data_headers": {"Content-Type": "application/x-www-form-urlencoded"},
        "data_auth": {"grant_type": "client_credentials"},
        "data_oauth_url": "https://example.com/data-oauth",
    }

    context.meta = None
    context.error = None
    context.mock_ingester_instance = None


@when('I call run_ingester in "once" mode')
def step_call_run_ingester_once(context):
    from src import api_wrapper

    # Patch the ApiIngester where it is imported/used (in src.api_wrapper)
    with patch("src.api_wrapper.ApiIngester") as MockIngester, patch(
        "src.api_wrapper.requests.post"
    ) as mock_post:
        # Mock oauth calls (wrapper calls retrieve_oauth_token -> requests.post)
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.side_effect = [
            {"access_token": "C1_TOKEN"},
            {"access_token": "DATA_TOKEN"},
        ]
        mock_post.return_value = resp

        instance = MockIngester.return_value
        instance.run_once.return_value = {"rows": 123}
        context.mock_ingester_instance = instance

        context.meta = api_wrapper.run_ingester(
            table=context.table,
            env=context.env,
            event=context.event,
            config=context.wrapper_config,
            run_mode="once",
        )


@then("ApiIngester run_once is called")
def step_assert_run_once_called(context):
    context.mock_ingester_instance.run_once.assert_called_once()
    # if you want stricter:
    context.mock_ingester_instance.run_once.assert_called_once_with(
        table=context.table, env_name=context.env
    )


@then("run_ingester returns the meta rows")
def step_assert_meta_rows(context):
    assert context.meta == {"rows": 123}


@when('I call run_ingester in "backfill" mode')
def step_call_run_ingester_backfill(context):
    from src import api_wrapper

    with patch("src.api_wrapper.ApiIngester") as MockIngester, patch(
        "src.api_wrapper.requests.post"
    ) as mock_post:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.side_effect = [
            {"access_token": "C1_TOKEN"},
            {"access_token": "DATA_TOKEN"},
        ]
        mock_post.return_value = resp

        instance = MockIngester.return_value
        instance.run_backfill.return_value = {"rows": 999}
        context.mock_ingester_instance = instance

        context.meta = api_wrapper.run_ingester(
            table=context.table,
            env=context.env,
            event=context.event,
            config=context.wrapper_config,
            run_mode="backfill",
            start="2025-01-01",
            end="2025-01-02",
        )


@then("ApiIngester run_backfill is called")
def step_assert_run_backfill_called(context):
    assert context.mock_ingester_instance.run_backfill.called


@when('I call run_ingester in "backfill" mode without dates')
def step_call_backfill_without_dates(context):
    from src import api_wrapper

    context.error = None
    try:
        api_wrapper.run_ingester(
            table=context.table,
            env=context.env,
            event=context.event,
            config=context.wrapper_config,
            run_mode="backfill",
            start=None,
            end=None,
        )
    except Exception as e:
        context.error = e


@then("run_ingester raises a ValueError")
def step_assert_value_error(context):
    assert isinstance(context.error, ValueError), f"Expected ValueError, got {context.error!r}"
