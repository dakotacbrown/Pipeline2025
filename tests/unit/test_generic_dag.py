# tests/unit/test_generic_ingester_dag.py

import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

DAG_FILE_RELATIVE_PATH = Path("dags/generic/generic_ingester.py")
DAG_MODULE_NAME = "generic_ingester_under_test"


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _project_root() -> Path:
    # tests live in <repo>/tests/unit/test_generic_ingester_dag.py
    return Path(__file__).resolve().parents[2]


def _load_module_from_path(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module spec from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _task_callable(task_obj):
    """
    Airflow TaskFlow @task returns an object that usually exposes the python function as:
      - .python_callable
      - or .function
      - or .__wrapped__
    """
    if hasattr(task_obj, "python_callable"):
        return task_obj.python_callable
    if hasattr(task_obj, "function"):
        return task_obj.function
    if hasattr(task_obj, "__wrapped__"):
        return task_obj.__wrapped__
    raise AttributeError(
        "Could not locate underlying callable for TaskFlow task"
    )


# --------------------------------------------------------------------------------------
# Scoped stubs (NO autouse): prevents impacting other DAG tests (like Salesforce triggerer)
# --------------------------------------------------------------------------------------
@pytest.fixture
def stub_common_modules(monkeypatch):
    """
    Stub only the modules imported by generic_ingester.
    Scoped to THIS file only (fixture is not autouse).
    """
    # Parent packages
    sys.modules.setdefault("dags", types.ModuleType("dags"))
    sys.modules.setdefault("dags.common", types.ModuleType("dags.common"))

    dag_utils = types.ModuleType("dags.common.dag_utilities")
    dag_utils.failover_managed_dag_tag = lambda: "failover-managed-dag"
    dag_utils.get_bucket_name = (
        lambda env, truncated_region: f"{env}-{truncated_region}-bucket"
    )
    dag_utils.get_cls_oauth_endpoint = lambda env: f"https://oauth/{env}"
    dag_utils.get_shairflow_environment = lambda: "qa"
    dag_utils.get_shairflow_region = lambda: "us-east-1"
    dag_utils.get_truncated_shairflow_region = lambda: "east"

    slack = types.ModuleType("dags.common.slack")
    slack.task_fail_slack_alert = lambda *args, **kwargs: None

    udf = types.ModuleType("dags.common.user_defined_filters")
    udf.ts_nodash_to_YYYYMMDDHHmmss = lambda s: s

    monkeypatch.setitem(sys.modules, "dags.common.dag_utilities", dag_utils)
    monkeypatch.setitem(sys.modules, "dags.common.slack", slack)
    monkeypatch.setitem(sys.modules, "dags.common.user_defined_filters", udf)

    yield


@pytest.fixture
def stub_provider_operators(monkeypatch):
    """
    Minimal provider operator stubs that:
      - support .partial().expand(...) / .partial().expand_kwargs(...)
      - support dependency wiring (>>) via update_relative + __rshift__/__lshift__
    """

    class _TaskLike:
        def __init__(self, task_id=None):
            self.task_id = task_id or "dummy"

        def update_relative(self, other, upstream=True):
            return None

        def __rshift__(self, other):
            self.update_relative(other, upstream=False)
            return other

        def __lshift__(self, other):
            self.update_relative(other, upstream=True)
            return other

    # Snowflake operator stub
    snowflake_mod = types.ModuleType(
        "airflow.providers.snowflake.operators.snowflake"
    )

    class SQLExecuteQueryOperator(_TaskLike):
        template_fields = ("sql",)

        def __init__(self, *args, **kwargs):
            super().__init__(task_id=kwargs.get("task_id"))
            self.kwargs = kwargs

        @classmethod
        def partial(cls, **kwargs):
            base = _TaskLike(task_id=kwargs.get("task_id"))

            def expand(**expand_kwargs):
                return _TaskLike(task_id=kwargs.get("task_id"))

            base.expand = expand
            return base

    snowflake_mod.SQLExecuteQueryOperator = SQLExecuteQueryOperator
    monkeypatch.setitem(
        sys.modules,
        "airflow.providers.snowflake.operators.snowflake",
        snowflake_mod,
    )

    # Glue operator stub
    glue_mod = types.ModuleType("airflow.providers.amazon.aws.operators.glue")

    class GlueJobOperator(_TaskLike):
        template_fields = ("job_name", "script_args")

        def __init__(self, *args, **kwargs):
            super().__init__(task_id=kwargs.get("task_id"))
            self.kwargs = kwargs

        @classmethod
        def partial(cls, **kwargs):
            base = _TaskLike(task_id=kwargs.get("task_id"))

            def expand_kwargs(mapped_kwargs):
                return _TaskLike(task_id=kwargs.get("task_id"))

            base.expand_kwargs = expand_kwargs
            return base

    glue_mod.GlueJobOperator = GlueJobOperator
    monkeypatch.setitem(
        sys.modules, "airflow.providers.amazon.aws.operators.glue", glue_mod
    )

    yield


@pytest.fixture
def dag_module(stub_common_modules, stub_provider_operators):
    path = _project_root() / DAG_FILE_RELATIVE_PATH
    return _load_module_from_path(DAG_MODULE_NAME, path)


# --------------------------------------------------------------------------------------
# Unit tests for helpers / tasks
# --------------------------------------------------------------------------------------
def test_deep_replace_placeholders_nested_preserves_missing(dag_module):
    fn = dag_module._deep_replace_placeholders

    data = {"a": "{{username}}", "b": {"c": ["{{client_secret}}", "x"]}}
    creds = {"username": "u1", "client_secret": None}

    out = fn(data, creds)

    assert out["a"] == "u1"
    # None should not replace placeholder (keeps placeholder)
    assert out["b"]["c"][0] == "{{client_secret}}"
    assert out["b"]["c"][1] == "x"


def test_get_sql_copy_replaces_target_table_and_strips_bucket(dag_module):
    get_sql_fn = _task_callable(dag_module.get_sql)

    sql_params = {"DATABASE": "DB", "SCHEMA": "SC"}
    template = (
        "COPY INTO {{ params.target_table }} FROM '@{{ params.s3_uri }}';"
    )
    m = mock_open(read_data=template)

    with patch("builtins.open", m):
        out = get_sql_fn(
            table_name="accounts",
            sql_params=sql_params,
            type="copy",
            s3_uri="s3://my-bucket/path/to/file.json",
            enabled=True,
        )

    assert "DB.SC.accounts" in out
    assert "@path/to/file.json" in out
    assert "my-bucket" not in out


def test_get_sql_disabled_returns_noop(dag_module):
    get_sql_fn = _task_callable(dag_module.get_sql)

    out = get_sql_fn(
        table_name="accounts",
        sql_params={"DATABASE": "DB", "SCHEMA": "SC"},
        type="deduplication",
        s3_uri=None,
        enabled=False,
    )
    assert out.strip().upper() == "SELECT 1;"


def test_build_exchange_extras_uses_c1_key(dag_module):
    """
    You renamed cl_* -> c1_*.
    This verifies the payload uses c1_oauth_url (and not cl_oauth_url).
    """
    fn = _task_callable(dag_module.build_exchange_extras)

    with patch.object(dag_module.Variable, "get", side_effect=["id", "secret"]):
        out = fn("qa")

    assert "c1_oauth_url" in out
    assert "cl_oauth_url" not in out


def test_get_latest_s3_uri_with_pattern_returns_newest(dag_module):
    get_latest_fn = _task_callable(dag_module.get_latest_s3_uri)

    s3 = MagicMock()
    paginator = MagicMock()
    s3.get_paginator.return_value = paginator

    paginator.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "prefix/a.txt",
                    "LastModified": datetime(2024, 1, 1, tzinfo=timezone.utc),
                },
                {
                    "Key": "prefix/b.csv",
                    "LastModified": datetime(2024, 1, 2, tzinfo=timezone.utc),
                },
            ]
        }
    ]

    with patch.object(dag_module.boto3, "client", return_value=s3):
        out = get_latest_fn("s3://my-bucket/prefix/", pattern="*.csv")

    assert out == "s3://my-bucket/prefix/b.csv"


def test_get_latest_s3_uri_with_pattern_no_matches_raises(dag_module):
    get_latest_fn = _task_callable(dag_module.get_latest_s3_uri)

    s3 = MagicMock()
    paginator = MagicMock()
    s3.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "prefix/a.txt",
                    "LastModified": datetime.now(timezone.utc),
                }
            ]
        }
    ]

    with patch.object(dag_module.boto3, "client", return_value=s3):
        with pytest.raises(ValueError):
            get_latest_fn("s3://my-bucket/prefix/", pattern="*.csv")


def test_get_latest_s3_uri_no_pattern_returns_latest_prefix(dag_module):
    get_latest_fn = _task_callable(dag_module.get_latest_s3_uri)

    s3 = MagicMock()
    paginator = MagicMock()
    s3.get_paginator.return_value = paginator

    def paginate_side_effect(**kwargs):
        # First call lists "directories"
        if kwargs.get("Delimiter") == "/":
            return [
                {
                    "CommonPrefixes": [
                        {"Prefix": "prefix/p1/"},
                        {"Prefix": "prefix/p2/"},
                    ]
                }
            ]
        # Then it probes each prefix for latest object time
        if kwargs.get("Prefix") == "prefix/p1/":
            return [
                {
                    "Contents": [
                        {
                            "Key": "prefix/p1/x",
                            "LastModified": datetime(
                                2024, 1, 1, tzinfo=timezone.utc
                            ),
                        }
                    ]
                }
            ]
        if kwargs.get("Prefix") == "prefix/p2/":
            return [
                {
                    "Contents": [
                        {
                            "Key": "prefix/p2/y",
                            "LastModified": datetime(
                                2024, 1, 3, tzinfo=timezone.utc
                            ),
                        }
                    ]
                }
            ]
        return [{"Contents": []}]

    paginator.paginate.side_effect = paginate_side_effect

    with patch.object(dag_module.boto3, "client", return_value=s3):
        out = get_latest_fn("s3://my-bucket/prefix/", pattern=None)

    assert out == "s3://my-bucket/prefix/p2/"


def test_get_latest_s3_uri_invalid_scheme_raises(dag_module):
    get_latest_fn = _task_callable(dag_module.get_latest_s3_uri)
    with pytest.raises(ValueError):
        get_latest_fn("http://not-s3/prefix/", pattern=None)


def test_resolve_run_config_minimum_shape(dag_module):
    """
    Your DAG uses resolve_run_config() to produce runtime config.
    This test verifies it returns a dict with the keys the DAG expects,
    without over-coupling to your exact internal structure.
    """
    resolve_fn = _task_callable(dag_module.resolve_run_config)

    fake_ctx = {
        "dag_run": types.SimpleNamespace(
            conf={"vendor": "revCloud", "credentials": {"username": "u"}}
        )
    }
    workflow_dict = {
        "INGESTER_TABLES": {"accounts": "dataset_accounts"},
        "INGESTER_DEDUPE": True,
        "INGESTER_TESTING": False,
        "INGESTER_SQL_PARAMS": {"DATABASE": "DB", "SCHEMA": "SC"},
    }

    with patch.object(
        dag_module, "get_current_context", return_value=fake_ctx
    ), patch.object(dag_module.Variable, "get", return_value=workflow_dict):
        out = resolve_fn()

    assert isinstance(out, dict)
    assert "workflow_dict" in out
    assert "vendor" in out
    assert out["vendor"] == "revcloud"
    assert isinstance(out["workflow_dict"], dict)


# --------------------------------------------------------------------------------------
# DAG import / graph-level tests
# --------------------------------------------------------------------------------------
def test_dag_builds_expected_tasks(dag_module):
    """
    Important:
    - When you call get_latest_s3_uri.override(task_id="latest_framework_zip"),
      the task id becomes "latest_framework_zip", NOT "get_latest_s3_uri".
    - Same for copy/dedupe SQL and latest_json.
    """
    dag = dag_module.dag
    task_ids = {t.task_id for t in dag.tasks}

    # Core runtime config
    assert "resolve_run_config" in task_ids

    # Your current TaskFlow ids (based on the set shown in your screenshot)
    assert "build_exchange_extras" in task_ids
    assert "build_env_vars" in task_ids
    assert "build_data_extras" in task_ids
    assert "build_event" in task_ids

    # Overridden/mapped task ids (these are the ones you should assert)
    assert "latest_framework_zip" in task_ids
    assert "glue_op_kwargs" in task_ids
    assert "run_glue_job" in task_ids
    assert "table_prefix" in task_ids
    assert "latest_json" in task_ids
    assert "copy_sql" in task_ids
    assert "load_table" in task_ids
    assert "dedupe_sql" in task_ids
    assert "dedupe_table" in task_ids

    # Optional: ensure old function-name task_ids are NOT expected
    assert "get_latest_s3_uri" not in task_ids
