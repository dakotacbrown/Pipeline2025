# tests/features/steps/step_job_runner.py

import io
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from behave import given, when, then

from src import run_step


class DummyLogger:
    def debug(self, *a, **k): ...
    def info(self, *a, **k): ...
    def warning(self, *a, **k): ...
    def error(self, *a, **k): ...
    def exception(self, *a, **k): ...


@given("a basic Glue event")
def step_basic_glue_event(context):
    context.event = {"foo": "bar"}


@when('I run the job runner in "once" mode')
def step_run_job_runner_once(context):
    # If you really want to silence logs, you *could* do:
    # patch("src.run_step.log", DummyLogger()), but it's optional.
    # For now, we won't patch logging at all to avoid import issues.

    # --- patch run_ingester used inside run_step ---
    fake_run_ingester = MagicMock(return_value={"rows": 42})
    # run_ingester is imported from src.api_wrapper inside run_step.main
    p_ingester = patch("src.api_wrapper.run_ingester", fake_run_ingester)
    context.patches.append(p_ingester)
    p_ingester.start()
    context.fake_run_ingester = fake_run_ingester

    # --- supply CLI args for _parse_args() ---
    argv = [
        "prog",
        "-y",
        "config/ingester.yml",
        "--table",
        "accounts",
        "--env",
        "dev",
        "--run_mode",
        "once",
        "--start_date",
        "2024-01-01",
        "--end_date",
        "2024-01-31",
    ]
    context._old_argv = sys.argv
    sys.argv = argv

    # --- override _parse_args so we can inject event ---
    fake_args = SimpleNamespace(
        yaml_path="config/ingester.yml",
        table="accounts",
        env_name="dev",
        run_mode="once",
        start_date="2024-01-01",
        end_date="2024-01-31",
        log_level="INFO",
        extra_env=["FOO=bar", "NO_EQUALS"],
        event=context.event,
    )
    p_args = patch("src.run_step._parse_args", lambda: fake_args)
    context.patches.append(p_args)
    p_args.start()

    # --- capture stdout & env ---
    context._old_stdout = sys.stdout
    context.stdout = io.StringIO()
    sys.stdout = context.stdout

    context._old_environ = os.environ.copy()
    os.environ.clear()

    # --- run system under test ---
    run_step.main()


@then("run_ingester is called with the expected arguments")
def step_check_run_ingester_call(context):
    context.fake_run_ingester.assert_called_once()
    _, kwargs = context.fake_run_ingester.call_args

    assert kwargs == {
        "table": "accounts",
        "env_name": "dev",
        "yaml_path": "config/ingester.yml",
        "event": context.event,
        "run_mode": "once",
        "start": "2024-01-01",
        "end": "2024-01-31",
    }

    assert os.environ["FOO"] == "bar"
    assert "NO_EQUALS" not in os.environ


@then("the job runner prints a successful status JSON")
def step_check_status_json(context):
    out = context.stdout.getvalue().strip()
    data = json.loads(out)
    assert data == {"status": "ok", "meta": {"rows": 42}}
