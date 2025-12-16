import os
from unittest.mock import MagicMock, patch
from behave import given, when, then


@given("a valid wrapper config and event")
def step_valid_wrapper_inputs(context):
    context.config = {
        "env_vars": {
            "dev": {"FOO": "bar"}
        }
    }

    context.event = {
        "c1_oauth_url": "https://oauth",
        "exchange_headers": {},
        "exchange_data": {},
        "data_headers": {},
        "data_auth": {},
        "data_auth_url": "https://oauth2",
    }


@when('run_ingester is called in "{mode}" mode')
def step_run_ingester_once(context, mode):
    fake_resp = MagicMock()
    fake_resp.raise_for_status.return_value = None
    fake_resp.json.return_value = {"access_token": "TOKEN"}

    fake_api = MagicMock()
    fake_api.run_once.return_value = {"rows": 1}

    with patch.dict(os.environ, {}, clear=True), \
         patch("src.api_wrapper.requests.post", return_value=fake_resp), \
         patch("src.api_wrapper.ApiIngester", return_value=fake_api):

        from src.api_wrapper import run_ingester

        context.result = run_ingester(
            table="table1",
            env="dev",
            event=context.event,
            config=context.config,
            run_mode=mode,
        )

        context.api = fake_api


@when('run_ingester is called in "backfill" mode with dates')
def step_run_backfill(context):
    fake_resp = MagicMock()
    fake_resp.raise_for_status.return_value = None
    fake_resp.json.return_value = {"access_token": "TOKEN"}

    fake_api = MagicMock()
    fake_api.run_backfill.return_value = {"rows": 2}

    with patch.dict(os.environ, {}, clear=True), \
         patch("src.api_wrapper.requests.post", return_value=fake_resp), \
         patch("src.api_wrapper.ApiIngester", return_value=fake_api):

        from src.api_wrapper import run_ingester

        run_ingester(
            table="table1",
            env="dev",
            event=context.event,
            config=context.config,
            run_mode="backfill",
            start="2024-01-01",
            end="2024-01-02",
        )

        context.api = fake_api


@when('run_ingester is called in "backfill" mode without dates')
def step_run_backfill_no_dates(context):
    context.error = None

    with patch("src.api_wrapper.ApiIngester"):
        from src.api_wrapper import run_ingester

        try:
            run_ingester(
                table="table1",
                env="dev",
                event=context.event,
                config=context.config,
                run_mode="backfill",
            )
        except Exception as e:
            context.error = e


@then("ApiIngester.run_once is invoked")
def step_assert_run_once(context):
    context.api.run_once.assert_called_once()


@then("ApiIngester.run_backfill is invoked")
def step_assert_run_backfill(context):
    context.api.run_backfill.assert_called_once()


@then("the wrapper returns metadata")
def step_assert_meta(context):
    assert context.result["rows"] == 1


@then("a ValueError is raised")
def step_assert_value_error(context):
    assert isinstance(context.error, ValueError)
