import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

DAG_FILE_RELATIVE_PATH = Path("dags/generic/generic_ingester.py")
DAG_MODULE_NAME = "generic_ingester_under_test"


def _project_root() -> Path:
    # Assumes tests live in: <repo>/tests/unit/test_generic_ingester_dag.py
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
    Airflow TaskFlow @task decorator exposes python callable as `.python_callable` on the task object.
    Depending on Airflow, it may also show up as `.function` or `.__wrapped__`.
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


@pytest.fixture
def stub_common_modules(monkeypatch):
    """
    Stub ONLY the modules that generic_ingester imports.
    This fixture is NOT autouse, so it won't affect other test files.
    """
    # Parent packages
    sys.modules.setdefault("dags", types.ModuleType("dags"))
    sys.modules.setdefault("dags.common", types.ModuleType("dags.common"))

    # dags.common.dag_utilities (only what generic_ingester imports)
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
    Provide minimal operator classes that:
      - allow .partial().expand(...) and .partial().expand_kwargs(...)
      - support dependency wiring (>>) by implementing update_relative + __rshift__/__lshift__
    This fixture is NOT autouse.
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


def test_deep_replace_placeholders_nested_leaves_none_intact(dag_module):
    fn = dag_module._deep_replace_placeholders

    data = {"a": "{{username}}", "b": {"c": ["{{client_secret}}", "x"]}}
    creds = {"username": "u1", "client_secret": None}

    out = fn(data, creds)
    assert out["a"] == "u1"
    assert out["b"]["c"][0] == "{{client_secret}}"
    assert out["b"]["c"][1] == "x"


def test_get_sql_copy_replaces_and_strips_bucket(dag_module):
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


def test_resolve_run_config_reads_workflow_tables_and_dedupe(dag_module):
    resolve_fn = _task_callable(dag_module.resolve_run_config)

    fake_ctx = {
        "dag_run": types.SimpleNamespace(
            conf={"vendor": "revCloud", "credentials": {"username": "u"}}
        )
    }

    workflow_dict = {
        "INGESTER_TABLES": {
            "accounts": "dataset_accounts",
            "contacts": "dataset_contacts",
        },
        "INGESTER_DEDUPE": True,
        "INGESTER_TESTING": False,
        "INGESTER_SQL_PARAMS": {"DATABASE": "DB", "SCHEMA": "SC"},
    }

    with patch.object(
        dag_module, "get_current_context", return_value=fake_ctx
    ), patch.object(dag_module.Variable, "get", return_value=workflow_dict):
        out = resolve_fn()

    assert out["vendor"] == "revcloud"
    assert out["dedupe"] is True
    assert out["testing"] is False
    assert out["credentials"]["username"] == "u"
    assert {"table": "accounts", "dataset_id": "dataset_accounts"} in out[
        "tables"
    ]
    assert {"table": "contacts", "dataset_id": "dataset_contacts"} in out[
        "tables"
    ]


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
        if kwargs.get("Delimiter") == "/":
            return [
                {
                    "CommonPrefixes": [
                        {"Prefix": "prefix/p1/"},
                        {"Prefix": "prefix/p2/"},
                    ]
                }
            ]
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


def test_dag_builds_expected_base_tasks(dag_module):
    dag = dag_module.dag
    task_ids = {t.task_id for t in dag.tasks}

    # stable task ids (we used override(task_id=...))
    assert "resolve_run_config" in task_ids
    assert "latest_framework_zip" in task_ids
    assert "build_event" in task_ids
    assert "glue_op_kwargs" in task_ids
    assert "run_glue_job" in task_ids
    assert "table_prefix" in task_ids
    assert "latest_json" in task_ids
    assert "copy_sql" in task_ids
    assert "load_table" in task_ids
    assert "dedupe_sql" in task_ids
    assert "dedupe_table" in task_ids
