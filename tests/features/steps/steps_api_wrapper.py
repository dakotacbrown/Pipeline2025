import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from behave import given, when, then

# make sure project root is on sys.path (if you need it)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import api_wrapper  # noqa: E402


@given("a basic API config and event")
def step_basic_api_config_event(context):
    context.event = {
        "env_vars": {"foo": "bar"},
        "c1_oauth_url": "https://c1-token.example.com",
        "exchange_headers": {"ex": "hdr"},
        "exchange_data": {"ex": "data"},
        "data_auth": {"c1_oauth_url": "https://c1-token.example.com"},
        "data_headers": {"hdr": "val"},
    }

    # patch YAML loader
    p_yaml = patch(
        "src.api_wrapper._load_yaml_from_anywhere",
        return_value={"apis": {"accounts": {}}},
    )

    # capture env vars
    env_vars_seen = {}

    def fake_set_env_vars(env_vars):
        env_vars_seen.update(env_vars)

    p_env_vars = patch("src.api_wrapper.set_env_vars_from_dict", fake_set_env_vars)

    # no real HTTP
    p_oauth = patch(
        "src.api_wrapper.retrieve_oauth_token",
        MagicMock(return_value="tok123"),
    )

    # MagicMock ApiIngester instance with REAL dict return values
    ingester_instance = MagicMock()
    ingester_instance.run_once.return_value = {"rows": 5}
    ingester_instance.run_backfill.return_value = {"mode": "backfill"}

    p_ingester = patch("src.api_wrapper.ApiIngester", return_value=ingester_instance)

    context.patches.extend([p_yaml, p_env_vars, p_oauth, p_ingester])
    for p in context.patches:
        p.start()

    context.env_vars_seen = env_vars_seen
    context.ingester_instance = ingester_instance

    context._old_environ = os.environ.copy()
    os.environ.clear()


@when('I call run_ingester in "once" mode')
def step_call_run_ingester_once(context):
    meta = api_wrapper.run_ingester(
        table="accounts",
        env_name="dev",
        yaml_path="config/ingester.yml",
        event=context.event,
        run_mode="once",
        start=None,
        end=None,
    )
    context.meta = meta


@when('I call run_ingester in "backfill" mode')
def step_call_run_ingester_backfill(context):
    meta = api_wrapper.run_ingester(
        table="accounts",
        env_name="prod",
        yaml_path="config/ingester.yml",
        event=context.event,
        run_mode="backfill",
        start="2024-01-01",
        end="2024-01-10",
    )
    context.meta = meta


@when('I call run_ingester in "backfill" mode without dates')
def step_call_run_ingester_backfill_no_dates(context):
    try:
        api_wrapper.run_ingester(
            table="accounts",
            env_name="dev",
            yaml_path="config/ingester.yml",
            event=context.event,
            run_mode="backfill",
            start="2024-01-01",
            end=None,
        )
    except Exception as exc:
        context.error = exc


@then("ApiIngester run_once is called")
def step_check_run_once_called(context):
    inst = context.ingester_instance
    inst.run_once.assert_called_once_with(
        table_name="accounts",
        env_name="dev",
    )
    inst.run_backfill.assert_not_called()

    assert context.env_vars_seen == {"foo": "bar"}
    assert os.environ["ENV"] == "dev"
    assert os.environ["TABLE"] == "accounts"
    assert os.environ["DATA_AUTH_TOKEN"] == "tok123"


@then("run_ingester returns the meta rows")
def step_check_meta_rows(context):
    assert context.meta == context.ingester_instance.run_once.return_value


@then("ApiIngester run_backfill is called")
def step_check_run_backfill_called(context):
    inst = context.ingester_instance
    inst.run_backfill.assert_called_once()
    inst.run_once.assert_not_called()
    assert os.environ["ENV"] == "prod"


@then("run_ingester raises a ValueError")
def step_check_value_error(context):
    assert isinstance(context.error, ValueError)
