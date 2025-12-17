import json
import sys
from unittest.mock import MagicMock, patch

from behave import given, then, when


@given("valid Glue CLI arguments")
def step_valid_cli(context):
    context.argv = [
        "run_step.py",
        "--env",
        "dev",
        "--vendor",
        "c1",
        "--table",
        "table1",
        "--event",
        json.dumps({"x": 1}),
        "--file_path",
        "config.yml",
        "--repo_name",
        "repo",
        "--github_token",
        "token",
    ]


@when("the job runner is executed")
def step_run_job(context):
    fake_github = MagicMock()
    fake_github.get_github_file_contents.return_value = {"cfg": "x"}

    with patch.object(sys, "argv", context.argv), patch(
        "src.run_step.GithubConnection", return_value=fake_github
    ), patch(
        "src.api_wrapper.run_ingester", return_value={"rows": 9}
    ) as run_ingester, patch(
        "builtins.print"
    ) as printer:

        from src.run_step import main

        main()

        context.run_ingester = run_ingester
        context.printer = printer


@then("run_ingester is called")
def step_assert_wrapper_called(context):
    context.run_ingester.assert_called_once()


@then("a success JSON is printed")
def step_assert_printed(context):
    printed = json.loads(context.printer.call_args.args[0])
    assert printed["status"] == "ok"
    assert printed["meta"]["rows"] == 9
