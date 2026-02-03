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
    # <repo>/tests/unit/test_generic_ingester_dag.py -> parents[2] == <repo>
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
    Airflow TaskFlow tasks typically expose the underlying callable via `.python_callable`.
    """
    if hasattr(task_obj, "python_callable"):
        return task_obj.python_callable
    if hasattr(task_obj, "__wrapped__"):
        return task_obj.__wrapped__
    raise AttributeError("Could not locate underlying callable for TaskFlow task")


@pytest.fixture
def stub_common_modules(monkeypatch):
    """
    Stub ONLY what this DAG imports from dags.common.*.
    Not autouse -> does not leak into other test modules.
    """
    sys.modules.setdefault("dags", types.ModuleType("dags"))
    sys.modules.setdefault("dags.common", types.ModuleType("dags.common"))

    dag_utils = types.ModuleType("dags.common.dag_utilities")
    dag_utils.failover_managed_dag_tag = lambda: "failover-managed-dag"
    dag_utils.get_bucket_name = lambda env, truncated_region: f"{env}-{truncated_region}-bucket"
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
    Critical: stub provider operators as real BaseOperator subclasses so:
      - dynamic task mapping (.partial().expand / expand_kwargs) works
      - dependency wiring uses Airflow's TaskMixin methods (edge_modifier safe)
    """
    from airflow.models.baseoperator import BaseOperator

    # ---- GlueJobOperator stub ----
    glue_mod = types.ModuleType("airflow.providers.amazon.aws.operators.glue")

    class GlueJobOperator(BaseOperator):
        template_fields = ("job_name", "script_args")

        def __init__(
            self,
            *,
            job_name: str | None = None,
            aws_conn_id: str | None = None,
            region_name: str | None = None,
            script_args: dict | None = None,
            wait_for_completion: bool = True,
            **kwargs,
        ):
            super().__init__(**kwargs)
            self.job_name = job_name
            self.aws_conn_id = aws_conn_id
            self.region_name = region_name
            self.script_args = script_args or {}
            self.wait_for_completion = wait_for_completion

        def execute(self, context):
            return None

    glue_mod.GlueJobOperator = GlueJobOperator
    monkeypatch.setitem(sys.modules, "airflow.providers.amazon.aws.operators.glue", glue_mod)

    # ---- SQLExecuteQueryOperator stub ----
    snowflake_mod = types.ModuleType("airflow.providers.snowflake.operators.snowflake")

    class SQLExecuteQueryOperator(BaseOperator):
        template_fields = ("sql",)

        def __init__(
            self,
            *,
            conn_id: str | None = None,
            sql: str | None = None,
            **kwargs,
        ):
            super().__init__(**kwargs)
            self.conn_id = conn_id
            self.sql = sql

        def execute(self, context):
            return None

    snowflake_mod.SQLExecuteQueryOperator = SQLExecuteQueryOperator
    monkeypatch.setitem(sys.modules, "airflow.providers.snowflake.operators.snowflake", snowflake_mod)

    yield


@pytest.fixture
def dag_module(stub_common_modules, stub_provider_operators):
    path = _project_root() / DAG_FILE_RELATIVE_PATH
    return _load_module_from_path(DAG_MODULE_NAME, path)


# ----------------------------
# Unit tests (pure functions)
# ----------------------------

def test_deep_replace_placeholders_nested_leaves_none_intact(dag_module):
    fn = dag_module._deep_replace_placeholders

    data = {"a": "{{username}}", "b": {"c": ["{{client_secret}}", "x"]}}
    creds = {"username": "u1", "client_secret": None}

    out = fn(data, creds)
    assert out["a"] == "u1"
    # None leaves the placeholder intact
    assert out["b"]["c"][0] == "{{client_secret}}"
    assert out["b"]["c"][1] == "x"


def test_resolve_run_config_requires_vendor(dag_module):
    resolve_fn = _task_callable(dag_module.resolve_run_config)
    fake_ctx = {"dag_run": types.SimpleNamespace(conf={})}

    with patch.object(dag_module, "get_current_context", return_value=fake_ctx):
        with pytest.raises(ValueError, match="vendor"):
            resolve_fn()


def test_resolve_run_config_requires_tables(dag_module):
    resolve_fn = _task_callable(dag_module.resolve_run_config)

    fake_ctx = {"dag_run": types.SimpleNamespace(conf={"vendor": "salesforce", "credentials": {}})}
    workflow_dict = {"INGESTER_TABLES": {}}

    with patch.object(dag_module, "get_current_context", return_value=fake_ctx), patch.object(
        dag_module.Variable, "get", return_value=workflow_dict
    ):
        with pytest.raises(ValueError, match="INGESTER_TABLES"):
            resolve_fn()


def test_resolve_run_config_happy_path_includes_dedupe_and_defaults(dag_module):
    resolve_fn = _task_callable(dag_module.resolve_run_config)

    fake_ctx = {
        "dag_run": types.SimpleNamespace(
            conf={
                "vendor": "revCloud",
                "credentials": {"username": "u"},
                "start_date": "2024-01-01",
            }
        )
    }

    workflow_dict = {
        "INGESTER_TABLES": {"accounts": "ds_accounts", "contacts": "ds_contacts"},
        "INGESTER_DEDUPE": True,
        "INGESTER_TESTING": False,
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "once",
        "INGESTER_SQL_PARAMS": {"DATABASE": "DB", "SCHEMA": "SC"},
        "INGESTER_ENV_VARS": {"X": "Y"},
        "INGESTER_CONFIG_PATH": "path/to/config.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
    }

    with patch.object(dag_module, "get_current_context", return_value=fake_ctx), patch.object(
        dag_module.Variable, "get", return_value=workflow_dict
    ):
        out = resolve_fn()

    assert out["vendor"] == "revcloud"
    assert out["dedupe"] is True
    assert out["testing"] is False
    assert out["credentials"]["username"] == "u"
    assert out["start_date"] == "2024-01-01"
    assert isinstance(out["tables"], list)
    assert {"table": "accounts", "dataset_id": "ds_accounts"} in out["tables"]


def test_extract_table_names_and_dataset_ids(dag_module):
    tnames = _task_callable(dag_module.extract_table_names)
    dids = _task_callable(dag_module.extract_dataset_ids)

    tables = [{"table": "a", "dataset_id": "da"}, {"table": "b", "dataset_id": "db"}]
    assert tnames(tables) == ["a", "b"]
    assert dids(tables) == ["da", "db"]


def test_get_latest_s3_uri_with_pattern_returns_newest_match(dag_module):
    fn = _task_callable(dag_module.get_latest_s3_uri)

    s3 = MagicMock()
    paginator = MagicMock()
    s3.get_paginator.return_value = paginator

    paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "prefix/a.txt", "LastModified": datetime(2024, 1, 1, tzinfo=timezone.utc)},
                {"Key": "prefix/b.csv", "LastModified": datetime(2024, 1, 2, tzinfo=timezone.utc)},
            ]
        }
    ]

    with patch.object(dag_module.boto3, "client", return_value=s3):
        out = fn("s3://my-bucket/prefix", pattern="*.csv")

    assert out == "s3://my-bucket/prefix/b.csv"


def test_get_latest_s3_uri_with_pattern_no_matches_raises(dag_module):
    fn = _task_callable(dag_module.get_latest_s3_uri)

    s3 = MagicMock()
    paginator = MagicMock()
    s3.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "prefix/a.txt", "LastModified": datetime.now(timezone.utc)}]}
    ]

    with patch.object(dag_module.boto3, "client", return_value=s3):
        with pytest.raises(ValueError):
            fn("s3://my-bucket/prefix/", pattern="*.csv")


def test_get_latest_s3_uri_no_pattern_returns_latest_prefix(dag_module):
    fn = _task_callable(dag_module.get_latest_s3_uri)

    s3 = MagicMock()
    paginator = MagicMock()
    s3.get_paginator.return_value = paginator

    def paginate_side_effect(**kwargs):
        if kwargs.get("Delimiter") == "/":
            return [{"CommonPrefixes": [{"Prefix": "prefix/p1/"}, {"Prefix": "prefix/p2/"}]}]
        if kwargs.get("Prefix") == "prefix/p1/":
            return [{"Contents": [{"Key": "prefix/p1/x", "LastModified": datetime(2024, 1, 1, tzinfo=timezone.utc)}]}]
        if kwargs.get("Prefix") == "prefix/p2/":
            return [{"Contents": [{"Key": "prefix/p2/y", "LastModified": datetime(2024, 1, 3, tzinfo=timezone.utc)}]}]
        return [{"Contents": []}]

    paginator.paginate.side_effect = paginate_side_effect

    with patch.object(dag_module.boto3, "client", return_value=s3):
        out = fn("s3://my-bucket/prefix/", pattern=None)

    assert out == "s3://my-bucket/prefix/p2/"


def test_get_latest_s3_uri_invalid_scheme_raises(dag_module):
    fn = _task_callable(dag_module.get_latest_s3_uri)
    with pytest.raises(ValueError):
        fn("http://not-s3/prefix/", pattern=None)


def test_get_sql_disabled_is_noop(dag_module):
    fn = _task_callable(dag_module.get_sql)
    out = fn(
        table_name="accounts",
        sql_params={"DATABASE": "DB", "SCHEMA": "SC"},
        type="deduplication",
        s3_uri=None,
        enabled=False,
    )
    assert out.strip().upper() == "SELECT 1;"


def test_get_sql_copy_replaces_target_table_and_strips_bucket(dag_module):
    fn = _task_callable(dag_module.get_sql)

    template = "COPY INTO {{ params.target_table }} FROM '@{{ params.s3_uri }}';"
    m = mock_open(read_data=template)

    with patch("builtins.open", m):
        out = fn(
            table_name="accounts",
            sql_params={"DATABASE": "DB", "SCHEMA": "SC"},
            type="copy",
            s3_uri="s3://my-bucket/path/to/file.json",
            enabled=True,
        )

    assert "DB.SC.accounts" in out
    assert "@path/to/file.json" in out
    assert "my-bucket" not in out


def test_build_exchange_extras_uses_c1_key(dag_module):
    fn = _task_callable(dag_module.build_exchange_extras)

    with patch.object(dag_module.Variable, "get", side_effect=["id", "secret"]):
        out = fn("qa")

    assert "c1_oauth_url" in out
    assert "exchange_data" in out
    assert out["exchange_data"]["client_id"] == "id"


def test_build_env_vars_sets_region_and_bucket(dag_module):
    fn = _task_callable(dag_module.build_env_vars)
    out = fn(region="us-east-1", bucket_name="b", ingester_env_vars={"X": "Y"})
    assert out["X"] == "Y"
    assert out["REGION"] == "us-east-1"
    assert out["BUCKET_NAME"] == "b"


def test_build_data_extras_placeholder_replacement(dag_module):
    fn = _task_callable(dag_module.build_data_extras)

    data_extras = {"auth": {"user": "{{username}}", "secret": "{{secret}}"}}
    creds = {"username": "u", "secret": None}

    out = fn(data_extras, creds)
    assert out["auth"]["user"] == "u"
    # None leaves placeholder intact
    assert out["auth"]["secret"] == "{{secret}}"


def test_build_glue_operator_kwargs_filters_none(dag_module):
    fn = _task_callable(dag_module.build_glue_operator_kwargs)

    out = fn(
        env="qa",
        region="us-east-1",
        vendor="salesforce",
        table="accounts",
        dataset_id="ds",
        event_json='{"x":1}',
        latest_zip_s3="s3://b/code/ETL/z.zip",
        etl_job_name="etl",
        etl_conn_name="conn",
        run_mode="once",
        repo_name="repo",
        config_path="path.yml",
        github_token=None,  # should be removed from script_args
        start_date="2024-01-01",
        end_date="2024-01-02",
        python_modules=None,  # should be removed
        index_url="https://index",
    )

    assert out["job_name"] == "etl"
    assert out["aws_conn_id"] == "conn"
    assert out["region_name"] == "us-east-1"
    assert out["task_id"] == "run_glue_job__accounts"
    assert "--github_token" not in out["script_args"]
    assert "--additional-python-modules" not in out["script_args"]


# ----------------------------
# DAG build/import test
# ----------------------------

def test_dag_builds_expected_tasks(dag_module):
    dag = dag_module.dag
    task_ids = {t.task_id for t in dag.tasks}

    # TaskFlow tasks
    assert "resolve_run_config" in task_ids
    assert "extract_table_names" in task_ids
    assert "extract_dataset_ids" in task_ids
    assert "get_latest_s3_uri" in task_ids
    assert "get_sql" in task_ids
    assert "build_exchange_extras" in task_ids
    assert "build_env_vars" in task_ids
    assert "build_data_extras" in task_ids
    assert "build_event_json_for_table" in task_ids
    assert "build_table_prefix" in task_ids
    assert "build_glue_operator_kwargs" in task_ids

    # Overridden task ids / operator tasks
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
