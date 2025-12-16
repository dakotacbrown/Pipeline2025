from __future__ import annotations

from behave import given, when, then
from unittest.mock import patch


# -------------------------
# Shared helpers
# -------------------------
def _start_patches(context):
    """
    Patch where the wrapper *uses* these symbols: src.api_wrapper.*
    """
    context._patches = []

    p_ingester = patch("src.api_wrapper.ApiIngester", autospec=True)
    MockIngester = p_ingester.start()
    context._patches.append(p_ingester)

    # Prevent requests.post from ever being hit via retrieve_oauth_token
    p_oauth = patch(
        "src.api_wrapper.retrieve_oauth_token",
        autospec=True,
        return_value="fake-token",
    )
    p_oauth.start()
    context._patches.append(p_oauth)

    return MockIngester


def _stop_patches(context):
    for p in reversed(getattr(context, "_patches", [])):
        try:
            p.stop()
        except Exception:
            pass
    context._patches = []


# -------------------------
# GIVEN
# -------------------------
@given("a basic API config and event")
def step_basic_api_config_and_event(context):
    # Avoid `context.table` (Behave can mask it). Use distinct names.
    context.table_key = "events_api"
    context.env_name = "dev"

    # Your wrapper expects: env_vars = event.get("env_vars", {}); env_vars[env]
    context.event = {
        "env_vars": {"dev": {"FOO": "bar"}},
        "exchange_headers": {},
        "exchange_data": {},
        "c1_oauth_url": "https://example.com/oauth",
        # Optional keys, only needed if your wrapper checks them:
        # "data_headers": {},
        # "data_auth": {},
        # "data_auth_url": "https://example.com/data-oauth",
    }

    context.config = {"some": "yaml-config"}
    context.result = None
    context.raised = None
    context.MockIngester = None


# -------------------------
# WHEN
# -------------------------
@when('I call run_ingester in "once" mode')
def step_call_run_ingester_once(context):
    context.MockIngester = _start_patches(context)
    inst = context.MockIngester.return_value
    inst.run_once.return_value = {"meta_rows": 123}

    from src.api_wrapper import run_ingester

    try:
        context.result = run_ingester(
            table=context.table_key,
            env=context.env_name,
            event=context.event,
            config=context.config,
            run_mode="once",
        )
    except Exception as e:
        context.raised = e
    finally:
        _stop_patches(context)


@when('I call run_ingester in "backfill" mode')
def step_call_run_ingester_backfill(context):
    context.MockIngester = _start_patches(context)
    inst = context.MockIngester.return_value
    inst.run_backfill.return_value = {"meta_rows": 999}

    from src.api_wrapper import run_ingester

    try:
        context.result = run_ingester(
            table=context.table_key,
            env=context.env_name,
            event=context.event,
            config=context.config,
            run_mode="backfill",
            start="2025-01-01",
            end="2025-01-02",
        )
    except Exception as e:
        context.raised = e
    finally:
        _stop_patches(context)


@when('I call run_ingester in "backfill" mode without dates')
def step_call_run_ingester_backfill_without_dates(context):
    context.MockIngester = _start_patches(context)

    from src.api_wrapper import run_ingester

    try:
        context.result = run_ingester(
            table=context.table_key,
            env=context.env_name,
            event=context.event,
            config=context.config,
            run_mode="backfill",
            # no start/end on purpose
        )
    except Exception as e:
        context.raised = e
    finally:
        _stop_patches(context)


# -------------------------
# THEN / AND
# -------------------------
@then("ApiIngester run_once is called")
def step_assert_run_once_called(context):
    assert context.raised is None, f"Unexpected exception: {context.raised}"
    inst = context.MockIngester.return_value
    assert inst.run_once.called, "Expected ApiIngester.run_once to be called"


@then("ApiIngester run_backfill is called")
def step_assert_run_backfill_called(context):
    assert context.raised is None, f"Unexpected exception: {context.raised}"
    inst = context.MockIngester.return_value
    assert inst.run_backfill.called, "Expected ApiIngester.run_backfill to be called"


@then("run_ingester returns the meta rows")
def step_assert_returns_meta_rows(context):
    assert context.raised is None, f"Unexpected exception: {context.raised}"
    assert context.result is not None, "Expected run_ingester to return a result"
    # keep this flexible: just ensure it looks like metadata came back
    assert isinstance(context.result, dict), "Expected metadata dict"
    assert len(context.result) > 0, "Expected non-empty metadata dict"


@then("run_ingester raises a ValueError")
def step_assert_value_error(context):
    assert context.raised is not None, "Expected an exception but none was raised"
    assert isinstance(
        context.raised, ValueError
    ), f"Expected ValueError, got {type(context.raised)}: {context.raised}"
