import os
from datetime import date
from unittest.mock import MagicMock, patch

from behave import given, when, then

# Import the function under test
from src.api_wrapper import run_ingester


def _mock_requests_post(token: str = "fake_token"):
    """
    Build a fake requests.post response object with:
      - raise_for_status()
      - json() -> {"access_token": token}
    """
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"access_token": token}
    return resp


@given("a valid wrapper config and event")
def step_valid_wrapper_inputs(context):
    # IMPORTANT: Behave reserves context.config internally.
    # Use a different attribute name to store your wrapper configuration.
    context.wrapper_config = {
        "env_vars": {
            "dev": {
                "FOO": "bar",
                # you can add anything else you expect to set into env here
            }
        }
    }

    # event keys based on what your wrapper reads in the screenshots
    context.event = {
        "c1_oauth_url": "https://example.com/c1/oauth",
        "exchange_headers": {"h": "v"},
        "exchange_data": {"grant_type": "client_credentials"},
        # optional path: if present, wrapper will fetch a 2nd token
        "data_headers": {"h2": "v2"},
        "data_auth": {"client_id": "abc", "client_secret": "xyz"},
        "data_auth_url": "https://example.com/data/oauth",
    }

    # defaults the scenarios can use
    context.table = "events_api"
    context.env = "dev"
    context.start = "2025-01-01"
    context.end = "2025-01-02"


@when('I call run_ingester in "{mode}" mode')
def step_call_run_ingester(context, mode):
    # isolate env mutations so tests don't leak across scenarios
    with patch.dict(os.environ, {}, clear=True):
        # Patch the ApiIngester constructor in the module where it's used (src.api_wrapper)
        # and patch requests.post (also used inside src.api_wrapper.retrieve_oauth_token).
        with patch("src.api_wrapper.ApiIngester") as MockIngester, patch(
            "src.api_wrapper.requests.post",
            return_value=_mock_requests_post("token123"),
        ):
            # configure ApiIngester instance behavior
            ingester_instance = MockIngester.return_value
            ingester_instance.run_once.return_value = {"meta_rows": 1}
            ingester_instance.run_backfill.return_value = {"meta_rows": 2}

            context._MockIngester = MockIngester
            context._ingester_instance = ingester_instance

            context.result = run_ingester(
                table=context.table,
                env=context.env,
                event=context.event,
                config=context.wrapper_config,
                run_mode=mode,
                start=context.start,
                end=context.end,
            )


@when('I call run_ingester in "{mode}" mode with dates')
def step_call_run_ingester_with_dates(context, mode):
    # just an explicit alias for readability in the feature file
    step_call_run_ingester(context, mode)


@when('I call run_ingester in "{mode}" mode without dates')
def step_call_run_ingester_without_dates(context, mode):
    with patch.dict(os.environ, {}, clear=True):
        with patch("src.api_wrapper.ApiIngester") as MockIngester, patch(
            "src.api_wrapper.requests.post",
            return_value=_mock_requests_post("token123"),
        ):
            context._MockIngester = MockIngester
            context._ingester_instance = MockIngester.return_value

            context.raised = None
            try:
                run_ingester(
                    table=context.table,
                    env=context.env,
                    event=context.event,
                    config=context.wrapper_config,
                    run_mode=mode,
                    start=None,
                    end=None,
                )
            except Exception as exc:  # behave-style capture
                context.raised = exc


@then("ApiIngester run_once is called")
def step_assert_run_once_called(context):
    context._MockIngester.assert_called_once()
    context._ingester_instance.run_once.assert_called_once_with(
        table=context.table,
        env_name=context.env,
    )


@then("ApiIngester run_backfill is called")
def step_assert_run_backfill_called(context):
    context._MockIngester.assert_called_once()

    # your wrapper parses YYYY-MM-DD into date objects
    expected_start = date.fromisoformat(context.start)
    expected_end = date.fromisoformat(context.end)

    context._ingester_instance.run_backfill.assert_called_once_with(
        table=context.table,
        env_name=context.env,
        start=expected_start,
        end=expected_end,
    )


@then("run_ingester returns the meta rows")
def step_assert_returns_meta(context):
    # We don't know your exact meta structure; we assert it returns whatever the ingester returns.
    # For once-mode in this file, we set run_once.return_value = {"meta_rows": 1}
    assert isinstance(context.result, dict)
    assert "meta_rows" in context.result


@then("run_ingester raises a ValueError")
def step_assert_raises_value_error(context):
    assert context.raised is not None, "Expected an exception but none was raised."
    assert isinstance(context.raised, ValueError), f"Expected ValueError, got {type(context.raised)}: {context.raised}"
