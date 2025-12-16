from __future__ import annotations

from typing import Any, Dict
from unittest.mock import patch

from behave import given, when, then


# IMPORTANT:
# - Do NOT use context.table (behave may set it internally for data tables).
# - Use context.table_key instead.
# - Patch where the symbols are USED (src.api_wrapper.*), not where they come from.


@given("a valid wrapper config and event")
def step_valid_wrapper_inputs(context) -> None:
    """
    Sets up the inputs expected by src.api_wrapper.run_ingester():
      - table (string)
      - env (string)
      - event (dict)
      - config (dict)
    """
    context.table_key = "events_api"
    context.env_name = "dev"

    # wrapper code expects: env_vars = event.get("env_vars", {}); env_vars[env]
    context.event = {
        "env_vars": {
            "dev": {
                "FOO": "bar",
                # Include any other env vars you want validated/propagated
            }
        },
        # wrapper code expects these keys (safe defaults)
        "exchange_headers": {},
        "exchange_data": {},
        "c1_oauth_url": "https://example.com/oauth",
        # Optional data auth keys (only used if both present)
        # "data_headers": {},
        # "data_auth": {},
        # "data_auth_url": "https://example.com/data-oauth",
    }

    # config is passed to ApiIngester(config=..., log=...)
    context.config = {"some": "yaml-config"}

    # patch holders
    context._patches = []
    context.result = None
    context.raised = None


@given("a basic API config and event")
def step_basic_api_config_and_event(context) -> None:
    """
    Your feature mentions "basic API config and event" for the error case.
    We'll still provide a valid event/config, but the error scenario will omit dates.
    """
    step_valid_wrapper_inputs(context)


@when('I call run_ingester in "once" mode')
def step_call_run_ingester_once(context) -> None:
    """
    - Patch ApiIngester so we don't invoke any internal network/IO.
    - Patch retrieve_oauth_token so we don't call requests.post.
    """
    # Patch the ingester class where api_wrapper imports/uses it
    p_ingester = patch("src.api_wrapper.ApiIngester", autospec=True)
    MockIngester = p_ingester.start()
    context._patches.append(p_ingester)

    # Patch oauth token retrieval to avoid requests
    p_oauth = patch("src.api_wrapper.retrieve_oauth_token", autospec=True, return_value="fake-token")
    p_oauth.start()
    context._patches.append(p_oauth)

    # Configure mock ingester return
    inst = MockIngester.return_value
    inst.run_once.return_value = {"rows": 123}

    from src.api_wrapper import run_ingester

    context.result = run_ingester(
        table=context.table_key,
        env=context.env_name,
        event=context.event,
        config=context.config,
        run_mode="once",
    )


@then("ApiIngester run_once is called")
def step_assert_run_once_called(context) -> None:
    from src import api_wrapper

    # api_wrapper.ApiIngester should have been patched, so this is a mock class
    api_wrapper.ApiIngester.assert_called_once()
    api_wrapper.ApiIngester.return_value.run_once.assert_called_once_with(
        table=context.table_key,
        env_name=context.env_name,
    )


@then("the wrapper returns metadata")
def step_assert_returns_metadata(context) -> None:
    assert context.result == {"rows": 123}


@then("run_ingester returns the meta rows")
def step_assert_returns_meta_rows(context) -> None:
    # If your old feature text expects this exact step
    assert context.result == {"rows": 123}


@when('I call run_ingester in "backfill" mode with dates')
def step_call_run_ingester_backfill(context) -> None:
    # Patch ingester
    p_ingester = patch("src.api_wrapper.ApiIngester", autospec=True)
    MockIngester = p_ingester.start()
    context._patches.append(p_ingester)

    # Patch oauth to avoid requests
    p_oauth = patch("src.api_wrapper.retrieve_oauth_token", autospec=True, return_value="fake-token")
    p_oauth.start()
    context._patches.append(p_oauth)

    inst = MockIngester.return_value
    inst.run_backfill.return_value = {"rows": 999}

    from src.api_wrapper import run_ingester

    context.result = run_ingester(
        table=context.table_key,
        env=context.env_name,
        event=context.event,
        config=context.config,
        run_mode="backfill",
        start="2025-01-01",
        end="2025-01-03",
    )


@then("ApiIngester run_backfill is called")
def step_assert_run_backfill_called(context) -> None:
    from src import api_wrapper

    api_wrapper.ApiIngester.assert_called_once()
    api_wrapper.ApiIngester.return_value.run_backfill.assert_called_once()
    # If you want the exact args:
    args, kwargs = api_wrapper.ApiIngester.return_value.run_backfill.call_args
    assert kwargs["table"] == context.table_key
    assert kwargs["env_name"] == context.env_name
    # start/end are dates inside wrapper; we won't over-constrain unless you want that


@when('I call run_ingester in "backfill" mode without dates')
def step_call_run_ingester_backfill_without_dates(context) -> None:
    # Patch ingester + oauth (even though it should error before ingest runs, safe to patch anyway)
    p_ingester = patch("src.api_wrapper.ApiIngester", autospec=True)
    p_ingester.start()
    context._patches.append(p_ingester)

    p_oauth = patch("src.api_wrapper.retrieve_oauth_token", autospec=True, return_value="fake-token")
    p_oauth.start()
    context._patches.append(p_oauth)

    from src.api_wrapper import run_ingester

    context.raised = None
    try:
        run_ingester(
            table=context.table_key,
            env=context.env_name,
            event=context.event,
            config=context.config,
            run_mode="backfill",
            start=None,
            end=None,
        )
    except Exception as e:
        context.raised = e


@then("a ValueError is raised")
def step_assert_value_error(context) -> None:
    assert context.raised is not None, "Expected an exception but none was raised"
    assert isinstance(context.raised, ValueError), f"Expected ValueError, got: {type(context.raised)}"


def after_scenario(context, scenario) -> None:
    """
    Behave will call this hook if it's present in the step module.
    (If you already have tests/features/environment.py, put this there instead.)
    """
    patches = getattr(context, "_patches", [])
    for p in reversed(patches):
        try:
            p.stop()
        except Exception:
            pass
    context._patches = []
