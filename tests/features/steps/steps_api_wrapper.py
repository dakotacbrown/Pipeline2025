import importlib
import json
import os
import sys
import types
from datetime import date

from behave import given, then, when

# -----------------------
# Fakes / helpers
# -----------------------


class FakeLogger:
    def __init__(self):
        self.infos = []
        self.warnings = []

    def info(self, msg, *args):
        self.infos.append(msg % args if args else msg)

    def warning(self, msg, *args):
        self.warnings.append(msg % args if args else msg)


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeRequests:
    def __init__(self):
        self.calls = []
        self.c1_token = "C1_TOKEN"
        self.data_token = "DATA_TOKEN"

    def post(self, url, headers=None, data=None, verify=None):
        self.calls.append(
            {
                "url": url,
                "headers": headers or {},
                "data": data or {},
                "verify": verify,
            }
        )
        # Return DATA token if the URL contains "data", otherwise C1 token
        if "data" in (url or ""):
            return FakeResponse({"access_token": self.data_token})
        return FakeResponse({"access_token": self.c1_token})


class FakeApiIngester:
    def __init__(self, config, log):
        self.config = config
        self.log = log
        self.once_calls = []
        self.backfill_calls = []

    def run_once(self, table_name, env_name):
        self.once_calls.append({"table_name": table_name, "env_name": env_name})
        return {"mode": "once", "table": table_name, "env": env_name}

    def run_backfill(self, table_name, env_name, start, end):
        assert isinstance(start, date)
        assert isinstance(end, date)
        self.backfill_calls.append(
            {
                "table_name": table_name,
                "env_name": env_name,
                "start": start,
                "end": end,
            }
        )
        return {
            "mode": "backfill",
            "table": table_name,
            "env": env_name,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }


def _make_pkg(name: str) -> types.ModuleType:
    """
    Behave tests often need to fake dotted imports.
    Setting __path__ makes ModuleType behave like a package.
    """
    m = types.ModuleType(name)
    m.__path__ = []
    return m


def _install_module(context, name: str, mod: types.ModuleType):
    sys.modules[name] = mod
    context._inserted_modules.append(name)


def install_fake_common_modules_for_wrapper(context):
    """
    Provides import targets used by src.api_wrapper at import-time:

      from asvc1scoredataservices_common.ingester.api_ingester import ApiIngester
      from asvc1scoredataservices_common.logger.basic_logger import setup_logger
    """
    fake_log = FakeLogger()
    context.fake_logger = fake_log

    # Package hierarchy
    root = _make_pkg("asvc1scoredataservices_common")
    ingester_pkg = _make_pkg("asvc1scoredataservices_common.ingester")
    logger_pkg = _make_pkg("asvc1scoredataservices_common.logger")

    # Leaf modules
    api_ingester_mod = types.ModuleType(
        "asvc1scoredataservices_common.ingester.api_ingester"
    )
    basic_logger_mod = types.ModuleType(
        "asvc1scoredataservices_common.logger.basic_logger"
    )

    def ApiIngester(config, log):
        inst = FakeApiIngester(config=config, log=log)
        context.fake_ingester = inst
        return inst

    def setup_logger():
        return fake_log

    api_ingester_mod.ApiIngester = ApiIngester
    basic_logger_mod.setup_logger = setup_logger

    # Install into sys.modules
    _install_module(context, "asvc1scoredataservices_common", root)
    _install_module(
        context, "asvc1scoredataservices_common.ingester", ingester_pkg
    )
    _install_module(
        context,
        "asvc1scoredataservices_common.ingester.api_ingester",
        api_ingester_mod,
    )
    _install_module(context, "asvc1scoredataservices_common.logger", logger_pkg)
    _install_module(
        context,
        "asvc1scoredataservices_common.logger.basic_logger",
        basic_logger_mod,
    )

    # Link hierarchy (helps attribute-based traversal)
    root.ingester = ingester_pkg
    root.logger = logger_pkg
    ingester_pkg.api_ingester = api_ingester_mod
    logger_pkg.basic_logger = basic_logger_mod


def _clean_wrapper_env():
    keys = {
        "C1_OAUTH_TOKEN",
        "DATA_OAUTH_TOKEN",
        "ENV",
        "TABLE",
        "START_DATE",
        "END_DATE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        # keys used in tests
        "FOO",
        "HELLO",
    }
    for k in list(os.environ.keys()):
        if k in keys:
            os.environ.pop(k, None)


# -----------------------
# Steps
# -----------------------


@given('the wrapper module is "{module_path}"')
def step_set_wrapper_module(context, module_path):
    context.wrapper_module = module_path


@given("I install fake common modules for the wrapper")
def step_install_common(context):
    install_fake_common_modules_for_wrapper(context)

    # Force wrapper to import fresh so it uses our faked setup_logger/ApiIngester
    sys.modules.pop(context.wrapper_module, None)
    context.wrapper = importlib.import_module(context.wrapper_module)


@given("I patch requests.post for oauth to return tokens")
def step_patch_requests(context):
    context.fake_requests = FakeRequests()
    # Patch the module-level requests ref in src.api_wrapper
    context.wrapper.requests = context.fake_requests


@when("I call run_ingester with parameters")
def step_set_params(context):
    row = context.table[0]

    context.params = {
        "table": (row.get("table") or "").strip(),
        "env": (row.get("env") or "").strip(),
        "run_mode": (row.get("run_mode") or "once").strip(),
        "start": (row.get("start") or "").strip() or None,
        "end": (row.get("end") or "").strip() or None,
    }
    context.expect_value_error = False


@when("I call run_ingester expecting ValueError with parameters")
def step_set_params_expect_error(context):
    step_set_params(context)
    context.expect_value_error = True


@when("the wrapper event is")
def step_set_event(context):
    context.event = json.loads(context.text)


@when("the wrapper config is")
def step_set_config_and_execute(context):
    context.config = json.loads(context.text)

    _clean_wrapper_env()

    try:
        context.meta = context.wrapper.run_ingester(
            table=context.params["table"],
            env=context.params["env"],
            event=context.event,
            config=context.config,
            run_mode=context.params["run_mode"],
            start=context.params["start"],
            end=context.params["end"],
        )
        context.raised = None
    except Exception as e:
        context.meta = None
        context.raised = e


@then('C1_OAUTH_TOKEN should equal "{token}"')
def step_c1_token(context, token):
    assert os.environ.get("C1_OAUTH_TOKEN") == token


@then('DATA_OAUTH_TOKEN should equal "{token}"')
def step_data_token(context, token):
    assert os.environ.get("DATA_OAUTH_TOKEN") == token


@then('ApiIngester should run_once with table "{table}" env "{env}"')
def step_ingester_run_once(context, table, env):
    inst = context.fake_ingester
    assert inst.once_calls, "Expected ApiIngester.run_once to be called"
    last = inst.once_calls[-1]
    assert last["table_name"] == table
    assert last["env_name"] == env
    assert inst.backfill_calls == [], "Expected run_backfill NOT to be called"


@then(
    'ApiIngester should run_backfill with table "{table}" env "{env}" start "{start}" end "{end}"'
)
def step_ingester_run_backfill(context, table, env, start, end):
    inst = context.fake_ingester
    assert inst.backfill_calls, "Expected ApiIngester.run_backfill to be called"
    last = inst.backfill_calls[-1]
    assert last["table_name"] == table
    assert last["env_name"] == env
    assert last["start"].isoformat() == start
    assert last["end"].isoformat() == end


@then("requests.post should be called {n:d} times")
def step_requests_called(context, n):
    assert len(context.fake_requests.calls) == n, context.fake_requests.calls


@then("a ValueError should have been raised")
def step_value_error(context):
    assert (
        context.raised is not None
    ), "Expected an exception but none was raised"
    assert isinstance(
        context.raised, ValueError
    ), f"Expected ValueError, got {type(context.raised)}"
