import importlib
import io
import json
import os
import sys
import tempfile
import types
from contextlib import redirect_stdout
from fnmatch import fnmatch
from pathlib import Path

from behave import given, then, when

# -----------------------
# Fakes
# -----------------------


class FakeLogger:
    def __init__(self):
        self.infos = []

    def info(self, msg, *args):
        try:
            rendered = msg % args if args else msg
        except Exception:
            rendered = (msg, args)
        self.infos.append(rendered)


class FakeGithubConnection:
    def __init__(self, log, token, repo):
        self.log = log
        self.token = token
        self.repo = repo
        self.fetched_paths = []
        self.contents_by_path = {}

    def get_github_file_contents(self, file_path):
        self.fetched_paths.append(file_path)
        return self.contents_by_path.get(file_path, "envs: {}\napis: {}")


class FakeWrapper:
    def __init__(self):
        self.calls = []

    def run_ingester(self, **kwargs):
        self.calls.append(kwargs)
        return {"meta_key": "meta_val"}


# -----------------------
# Module install helpers
# -----------------------


def _make_pkg(name: str) -> types.ModuleType:
    m = types.ModuleType(name)
    m.__path__ = []  # make it a package
    return m


def _install_module(context, name: str, module: types.ModuleType):
    sys.modules[name] = module
    context._inserted_modules.append(name)


def install_fake_common_and_api_wrapper(context):
    """
    Provides import targets used inside runner.main():
      - asvc1scoredataservices_common.github.common.GithubConnection
      - asvc1scoredataservices_common.logger.basic_logger.setup_logger
      - src.api_wrapper.run_ingester
    """
    fake_log = FakeLogger()
    fake_wrapper = FakeWrapper()

    # ---- asvc common packages
    root = _make_pkg("asvc1scoredataservices_common")
    github_pkg = _make_pkg("asvc1scoredataservices_common.github")
    logger_pkg = _make_pkg("asvc1scoredataservices_common.logger")

    github_common = types.ModuleType(
        "asvc1scoredataservices_common.github.common"
    )
    basic_logger = types.ModuleType(
        "asvc1scoredataservices_common.logger.basic_logger"
    )

    # GithubConnection factory so we can capture instance per scenario
    def GithubConnection(log, github_token, repo_name):
        gh = FakeGithubConnection(log, github_token, repo_name)
        # allow tests to pre-seed yaml contents
        gh.contents_by_path.update(getattr(context, "yaml_by_path", {}))
        context.fake_github = gh
        return gh

    def setup_logger():
        context.fake_logger = fake_log
        return fake_log

    github_common.GithubConnection = GithubConnection
    basic_logger.setup_logger = setup_logger

    _install_module(context, "asvc1scoredataservices_common", root)
    _install_module(context, "asvc1scoredataservices_common.github", github_pkg)
    _install_module(
        context, "asvc1scoredataservices_common.github.common", github_common
    )
    _install_module(context, "asvc1scoredataservices_common.logger", logger_pkg)
    _install_module(
        context,
        "asvc1scoredataservices_common.logger.basic_logger",
        basic_logger,
    )

    # Link hierarchy
    root.github = github_pkg
    root.logger = logger_pkg
    github_pkg.common = github_common
    logger_pkg.basic_logger = basic_logger

    # ---- src.api_wrapper
    # IMPORTANT: do NOT blindly overwrite sys.modules["src"].
    # If the real src package exists, use it. If not, create a package-like one.
    try:
        src_pkg = importlib.import_module("src")
    except ModuleNotFoundError:
        src_pkg = _make_pkg("src")
        _install_module(context, "src", src_pkg)

    api_wrapper_mod = types.ModuleType("src.api_wrapper")

    def run_ingester(**kwargs):
        return fake_wrapper.run_ingester(**kwargs)

    api_wrapper_mod.run_ingester = run_ingester
    _install_module(context, "src.api_wrapper", api_wrapper_mod)

    # Attach as attribute if possible
    try:
        setattr(src_pkg, "api_wrapper", api_wrapper_mod)
    except Exception:
        pass

    context.fake_wrapper = fake_wrapper


# -----------------------
# Steps: setup_path component checks
# -----------------------


@given('I have a sys.path list containing "{zip_path}"')
def step_sys_path_contains_zip(context, zip_path):
    context.sys_path = ["/something/else", zip_path, "/another"]


@given("I have a sys.path list {items}")
def step_sys_path_list_literal(context, items):
    context.sys_path = json.loads(items)


@given("I have an empty temp search directory")
def step_empty_temp_dir(context):
    td = tempfile.TemporaryDirectory()
    context._tmpdir = td
    context.search_dir = Path(td.name)


@given("I have a temp search directory with zips")
def step_temp_dir_with_zips(context):
    td = tempfile.TemporaryDirectory()
    context._tmpdir = td
    d = Path(td.name)
    for row in context.table:
        (d / row["name"]).write_bytes(b"")  # file just needs to exist
    context.search_dir = d


@when('I call setup_path with pattern "{pattern}"')
def step_call_setup_path(context, pattern):
    runner = importlib.import_module(
        getattr(context, "runner_module", "src.run_step")
    )
    search_dirs = (
        [context.search_dir]
        if hasattr(context, "search_dir")
        else [Path("/tmp")]
    )
    runner.setup_path(
        pattern=pattern, search_dirs=search_dirs, sys_path=context.sys_path
    )


@when('I call setup_path with pattern "{pattern}" (capturing errors)')
def step_call_setup_path_capture(context, pattern):
    runner = importlib.import_module(
        getattr(context, "runner_module", "src.run_step")
    )
    search_dirs = (
        [context.search_dir]
        if hasattr(context, "search_dir")
        else [Path("/tmp")]
    )
    try:
        runner.setup_path(
            pattern=pattern, search_dirs=search_dirs, sys_path=context.sys_path
        )
        context.caught_exc = None
    except Exception as e:
        context.caught_exc = e


@then('the first sys.path entry should be "{expected}"')
def step_first_sys_path(context, expected):
    assert (
        context.sys_path[0] == expected
    ), f"Expected first entry {expected}, got {context.sys_path[0]}"


@then('sys.path should contain "{expected}"')
def step_sys_path_contains(context, expected):
    assert (
        expected in context.sys_path
    ), f"Expected sys.path to contain {expected}. Got: {context.sys_path}"


@then("sys.path should remain {items}")
def step_sys_path_unchanged(context, items):
    expected = json.loads(items)
    assert (
        context.sys_path == expected
    ), f"Expected {expected}, got {context.sys_path}"


@then("setup_path should raise ValueError")
def step_setup_path_raises_value_error(context):
    assert (
        getattr(context, "caught_exc", None) is not None
    ), "Expected an exception but none was caught"
    assert isinstance(
        context.caught_exc, ValueError
    ), f"Expected ValueError, got {type(context.caught_exc)}"


# -----------------------
# Steps: main() end-to-end with fakes
# -----------------------


@given('the runner module is "{module_path}"')
def step_set_runner_module(context, module_path):
    context.runner_module = module_path


@given("I install fake common modules and fake api_wrapper")
def step_install_fakes(context):
    install_fake_common_and_api_wrapper(context)


@given("I patch runner.setup_path to be a no-op")
def step_patch_setup_path_noop(context):
    runner = importlib.import_module(context.runner_module)
    context.runner = runner
    runner.setup_path = lambda *a, **k: None  # no-op


@when("I run runner.main with argv")
def step_run_main_with_argv(context):
    runner = getattr(
        context, "runner", importlib.import_module(context.runner_module)
    )

    argv = []
    for row in context.table:
        argv.append(row["arg"])
        argv.append(row["value"])

    buf = io.StringIO()
    with redirect_stdout(buf):
        runner.main(argv)

    context.stdout = buf.getvalue().strip()


@then('stdout JSON should have status "{status}"')
def step_stdout_json_status(context, status):
    payload = json.loads(context.stdout)
    assert (
        payload["status"] == status
    ), f"Expected status={status}, got {payload}"
    assert "meta" in payload, f"Expected meta in payload, got {payload}"


@then(
    'run_ingester should be called with table "{table}" env "{env}" run_mode "{run_mode}"'
)
def step_run_ingester_called(context, table, env, run_mode):
    calls = context.fake_wrapper.calls
    assert calls, "Expected run_ingester to be called"
    last = calls[-1]
    assert last["table"] == table
    assert last["env"] == env
    assert last["run_mode"] == run_mode
    assert isinstance(
        last["event"], dict
    ), f"Expected event dict, got {type(last['event'])}"


@then('run_ingester should receive start "{start}" and end "{end}"')
def step_run_ingester_start_end(context, start, end):
    last = context.fake_wrapper.calls[-1]
    assert last["start"] == start
    assert last["end"] == end


@then('GithubConnection should fetch file_path "{file_path}"')
def step_github_fetch_path(context, file_path):
    assert hasattr(context, "fake_github"), "Expected fake_github to exist"
    assert (
        context.fake_github.fetched_paths
    ), "Expected get_github_file_contents to be called"
    assert context.fake_github.fetched_paths[-1] == file_path
