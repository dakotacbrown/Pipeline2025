import os
from datetime import date
from unittest.mock import MagicMock, patch

from behave import given, when, then

from src.api_wrapper import run_ingester


# ------------------------
# GIVEN
# ------------------------

@given("a valid wrapper config and event")
def step_valid_wrapper_config(context):
    # DO NOT use context.config (reserved by behave)
    context.wrapper_config = {
        "env_vars": {
            "dev": {
                "FOO": "bar",
            }
        }
    }

    context.event = {
        "c1_oauth_url": "https://example.com/oauth",
        "exchange_headers": {"h": "v"},
        "exchange_data": {"grant_type": "client_credentials"},
    }

    context.table = "events_api"
    context.env = "dev"
    context.start = "2024-01-01"
    context.end = "2024-01-02"


# ------------------------
# WHEN
# ------------------------

@when('run_ingester is called in "{mode}" mode')
def step_run_ingester_once(context, mode):
    with patch.dict(os.environ, {}, clear=True), \
         patch("src.api_wrapper.requests.post") as mock_post, \
         patch("src.api_wrapper.ApiIngester") as MockIngester:

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"access_token": "token"}
        mock_post.return_value = mock_resp

        ingester = MockIngester.return_value
        ingester.run_once.return_value = {"rows": 1}
        ingester.run_backfill.return_value = {"rows": 2}

        context.MockIngester = MockIngester
        context.ingester = ingester

        context.result = run_ingester(
            table=context.table,
            env=context.env,
            event=context.event,
            config=context.wrapper_config,
            run_mode=mode,
            start=context.start,
            end=context.end,
        )


@when('run_ingester is called in "backfill" mode with dates')
def step_run_ingester_backfill(context):
    step_run_ingester_once(context, "backfill")


@when('run_ingester is called in "backfill" mode without dates')
def step_run_ingester_backfill_no_dates(context):
    context.raised = None

    with patch.dict(os.environ, {}, clear=True), \
         patch("src.api_wrapper.requests.post"), \
         patch("src.api_wrapper.ApiIngester"):

        try:
            run_ingester(
                table=context.table,
                env=context.env,
                event=context.event,
                config=context.wrapper_config,
                run_mode="backfill",
                start=None,
                end=None,
            )
        except Exception as exc:
            context.raised = exc


# ------------------------
# THEN
# ------------------------

@then("ApiIngester.run_once is invoked")
def step_assert_run_once(context):
    context.ingester.run_once.assert_called_once_with(
        table=context.table,
        env_name=context.env,
    )


@then("ApiIngester.run_backfill is invoked")
def step_assert_run_backfill(context):
    context.ingester.run_backfill.assert_called_once_with(
        table=context.table,
        env_name=context.env,
        start=date.fromisoformat(context.start),
        end=date.fromisoformat(context.end),
    )


@then("the wrapper returns metadata")
def step_assert_returns_metadata(context):
    assert isinstance(context.result, dict)
    assert context.result


@then("a ValueError is raised")
def step_assert_value_error(context):
    assert context.raised is not None
    assert isinstance(context.raised, ValueError)
