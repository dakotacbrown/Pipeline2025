import io
import json
import os
import sys
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
    # You can add real fields later if needed.
    context.event = {"foo": "bar"}


@when('I run the job runner in "once" mode')
def step_run_job_runner_once(context):
    # --- patch logging so tests don't reconfigure global logging ---
    p_basic = patch("run_step.logging.basicConfig", lambda *a, **k: None)
    p_logger = patch("run_step.logging.getLogger", return_value=DummyLogger())
    context.patches.extend([p_basic, p_logger])
    for p in context.patches:
        p.start()

    # --- patch run_ingester imported by run_step (component boundary) ---
    fake_run_ingester = MagicMock(return_value={"rows": 42})
    p_ingester = patch("run_step.run_ingester", fake_run_ingester)
    context.patches.append(p_ingester)
    p_ingester.start()
    context.fake_run_ingester = fake_run_ingester

    # --- set CLI args for _parse_args() ---
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

    # --- patch _parse_args so we can inject the event directly ---
    from types import SimpleNamespace

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
    p_args = patch("run_step._parse_args", lambda: fake_args)
    context.patches.append(p_args)
    p_args.start()

    # --- capture stdout & isolate environment ---
    context._old_stdout = sys.stdout
    context.stdout = io.StringIO()
    sys.stdout = context.stdout

    context._old_environ = os.environ.copy()
    os.environ.clear()

    # --- run main (system under test) ---
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

    # env vars from extra_env
    assert os.environ["FOO"] == "bar"
    assert "NO_EQUALS" not in os.environ


@then("the job runner prints a successful status JSON")
def step_check_status_json(context):
    out = context.stdout.getvalue().strip()
    data = json.loads(out)
    assert data == {"status": "ok", "meta": {"rows": 42}}
