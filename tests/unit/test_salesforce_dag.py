# tests/unit/test_salesforce_ingester_dag.py
from __future__ import annotations

import importlib
import sys
import types
from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest

DAG_IMPORT = "dags.salesforce.salesforce_ingester"


class _DummyDag:
    def __init__(self, *a, **k):
        self.args = a
        self.kwargs = k


def _dag_decorator(*dargs, **dkwargs):
    # @dag(...) def f(): ...
    def _wrap(fn):
        def _inner(*a, **k):
            return _DummyDag(dargs, dkwargs)

        return _inner

    return _wrap


class _TaskWrapper:
    """
    Minimal Airflow TaskFlow wrapper that supports:
      - calling the task: task_fn(arg) -> xcom placeholder
      - .override(task_id=...) returning a callable
    """

    def __init__(self, fn):
        self.fn = fn
        self.task_id = getattr(fn, "__name__", "task")

    def override(self, task_id: str, **_):
        def _call(*a, **k):
            # record override id so tests can assert it was built correctly
            self.task_id = task_id
            # Return an XCom-like placeholder (Airflow would return XComArg)
            return {
                "__xcom_task_id__": task_id,
                "__xcom_value__": self.fn(*a, **k),
            }

        return _call

    def __call__(self, *a, **k):
        return {
            "__xcom_task_id__": self.task_id,
            "__xcom_value__": self.fn(*a, **k),
        }


def _task_decorator(*targs, **tkwargs):
    # @task(...) def f(): ...
    def _wrap(fn):
        return _TaskWrapper(fn)

    return _wrap


class _DummyGlueJobOperator:
    created: List["_DummyGlueJobOperator"] = []

    def __init__(self, *a, **k):
        self.args = a
        self.kwargs = k
        _DummyGlueJobOperator.created.append(self)

    # allow "latest_zip >> build_event >> run_glue" chaining
    def __rrshift__(self, other):
        return self

    def __rshift__(self, other):
        return other


class _DummySnowflakeHook:
    conn_type = "snowflake"

    def __init__(self, *a, **k):
        self.args = a
        self.kwargs = k

    def get_conn(self):
        # truthy means "configured"
        return object()


@pytest.fixture()
def mod(monkeypatch):
    """
    Import the DAG module with Airflow/boto3/etc stubbed out so unit tests
    don't require an Airflow runtime.
    """

    # ---- stub airflow decorators
    airflow_decorators = types.ModuleType("airflow.decorators")
    airflow_decorators.dag = _dag_decorator
    airflow_decorators.task = _task_decorator

    # ---- stub airflow.models.Variable
    airflow_models = types.ModuleType("airflow.models")

    class _Variable:
        _store: Dict[str, Any] = {}

        @classmethod
        def get(cls, key, default_var=None):
            return cls._store.get(key, default_var)

    airflow_models.Variable = _Variable

    # ---- stub providers operators/hooks
    aws_glue_ops = types.ModuleType(
        "airflow.providers.amazon.aws.operators.glue"
    )
    aws_glue_ops.GlueJobOperator = _DummyGlueJobOperator

    snowflake_hook_mod = types.ModuleType(
        "airflow.providers.snowflake.hooks.snowflake"
    )
    snowflake_hook_mod.SnowflakeHook = _DummySnowflakeHook

    # ---- stub dags.common.dag_utilities functions used
    dag_utils = types.ModuleType("dags.common.dag_utilities")

    def failover_managed_dag_tag():
        return "failover_managed"

    def get_bucket_name(env: str, region: str) -> str:
        return "c1scoredataservices-qa-east"

    def get_c1s_oauth_endpoint(env: str) -> str:
        return "https://c1/oauth"

    def get_shairflow_environment() -> str:
        return "qa"

    def get_shairflow_region() -> str:
        return "us-east-1"

    def get_truncated_shairflow_region() -> str:
        return "east"

    def sql_transformation_task_group(**kwargs):
        # Return an operator-like placeholder that can be chained
        tg = types.SimpleNamespace(kwargs=kwargs)

        def _rshift(other):
            return other

        tg.__rshift__ = _rshift
        tg.__rrshift__ = lambda other: tg
        return tg

    def successful_execution_status(**kwargs):
        return kwargs

    dag_utils.failover_managed_dag_tag = failover_managed_dag_tag
    dag_utils.get_bucket_name = get_bucket_name
    dag_utils.get_c1s_oauth_endpoint = get_c1s_oauth_endpoint
    dag_utils.get_shairflow_environment = get_shairflow_environment
    dag_utils.get_shairflow_region = get_shairflow_region
    dag_utils.get_truncated_shairflow_region = get_truncated_shairflow_region
    dag_utils.sql_transformation_task_group = sql_transformation_task_group
    dag_utils.successful_execution_status = successful_execution_status

    # ---- stub slack + filters imports
    slack_mod = types.ModuleType("dags.common.slack")
    slack_mod.task_fail_slack_alert = lambda *a, **k: None

    filters_mod = types.ModuleType("dags.common.user_defined_filters")
    filters_mod.ts_nodash_to_YYYYMMDDHHmmss = lambda x: x

    # ---- stub pendulum (only duration/timezone used)
    pendulum_mod = types.ModuleType("pendulum")

    class _Duration:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    pendulum_mod.duration = lambda **kwargs: _Duration(**kwargs)
    pendulum_mod.timezone = lambda tz: tz

    # ---- stub boto3
    boto3_mod = types.ModuleType("boto3")

    class _Paginator:
        def __init__(self, pages):
            self._pages = pages

        def paginate(self, **kwargs):
            # ignore kwargs, return canned pages
            return iter(self._pages)

    class _S3Client:
        def __init__(self, pages):
            self._pages = pages

        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return _Paginator(self._pages)

    # default: no results unless overridden in test
    boto3_mod._pages = []
    boto3_mod.client = lambda service: _S3Client(boto3_mod._pages)

    # ---- install all stubs
    monkeypatch.setitem(sys.modules, "airflow.decorators", airflow_decorators)
    monkeypatch.setitem(sys.modules, "airflow.models", airflow_models)
    monkeypatch.setitem(
        sys.modules, "airflow.providers.amazon.aws.operators.glue", aws_glue_ops
    )
    monkeypatch.setitem(
        sys.modules,
        "airflow.providers.snowflake.hooks.snowflake",
        snowflake_hook_mod,
    )
    monkeypatch.setitem(sys.modules, "dags.common.dag_utilities", dag_utils)
    monkeypatch.setitem(sys.modules, "dags.common.slack", slack_mod)
    monkeypatch.setitem(
        sys.modules, "dags.common.user_defined_filters", filters_mod
    )
    monkeypatch.setitem(sys.modules, "pendulum", pendulum_mod)
    monkeypatch.setitem(sys.modules, "boto3", boto3_mod)

    # ---- now import your module under test
    if DAG_IMPORT in sys.modules:
        del sys.modules[DAG_IMPORT]
    m = importlib.import_module(DAG_IMPORT)
    importlib.reload(m)
    return m


def test_safe_task_id_sanitizes(mod):
    assert mod._safe_task_id("Opportunity") == "opportunity"
    assert mod._safe_task_id("Opportunity__c") == "opportunity__c"
    assert mod._safe_task_id("  weird--Name!! ") == "weird--name"


def test_safe_json_loads_json_and_python_literal(mod):
    assert mod._safe_json_loads('{"a": 1}', {}) == {"a": 1}
    # fallback: python literal (single quotes)
    assert mod._safe_json_loads("{'a': 1}", {}) == {"a": 1}
    # empty => default
    assert mod._safe_json_loads("", {"d": 1}) == {"d": 1}


def test_get_latest_framework_zip_picks_newest(mod, monkeypatch):
    # patch boto3 pages used by get_latest_framework_zip
    boto3_mod = sys.modules["boto3"]

    boto3_mod._pages = [
        {
            "Contents": [
                {
                    "Key": "code/ETL/debi-etl-framework-glue-0.0.0-older.zip",
                    "LastModified": datetime(2025, 12, 1, tzinfo=timezone.utc),
                },
                {
                    "Key": "code/ETL/debi-etl-framework-glue-0.0.0-newer.zip",
                    "LastModified": datetime(2025, 12, 2, tzinfo=timezone.utc),
                },
            ]
        }
    ]

    s3_uri = mod.get_latest_framework_zip("s3://my-bucket/code/ETL/")
    assert (
        s3_uri
        == "s3://my-bucket/code/ETL/debi-etl-framework-glue-0.0.0-newer.zip"
    )


def test_get_latest_framework_zip_raises_when_none(mod, monkeypatch):
    boto3_mod = sys.modules["boto3"]
    boto3_mod._pages = [{"Contents": []}]
    with pytest.raises(ValueError):
        mod.get_latest_framework_zip("s3://my-bucket/code/ETL/")


def test_dag_builds_event_task_and_glue_args_are_jinja_templates(
    mod, monkeypatch
):
    # Set Variables that your module reads
    Variable = sys.modules["airflow.models"].Variable
    # workflow dict must be a JSON-parseable string
    Variable._store["INGESTER_WORKFLOW_SALESFORCE"] = (
        '{"INGESTER_TABLES": "[\\"opportunity\\"]",'
        ' "INGESTER_CONFIG_PATH": "cfg.yml",'
        ' "INGESTER_CONFIG_REPO_NAME": "repo",'
        ' "INGESTER_RUN_MODE": "once",'
        ' "INGESTER_GLUE_JOB_NAME": "etl-job",'
        ' "INGESTER_GLUE_CONN_NAME": "etl-net-conn",'
        ' "INGESTER_PYTHON_MODULE": "c1-asvc1scoredataservices-common==0.1.41",'
        ' "INGESTER_ENV_VARS": "{\\"X_UPSTREAM_ENV\\": {\\"AWS_REGION\\": \\"us-east-1\\"}}"'
        "}"
    )
    Variable._store["C1SCOREDATASERVICES_GITHUB_PASSWORD"] = "gh-token"
    Variable._store["C1SCOREDATASERVICES_EXCHANGE_ID"] = "ex-id"
    Variable._store["C1SCOREDATASERVICES_EXCHANGE_SECRET"] = "ex-secret"
    Variable._store["C1S_SALESFORCE_USERNAME"] = "sf-user"
    Variable._store["C1S_SALESFORCE_PASSWORD"] = "sf-pass"
    Variable._store["C1S_SALESFORCE_CLIENTID"] = "sf-client"
    Variable._store["C1S_SALESFORCE_CLIENTSECRET"] = "sf-client-secret"
    # enable sql task group
    Variable._store["INGESTER_COPY_SQL"] = "copy.sql"

    # Clear any previously created GlueJobOperator instances
    _DummyGlueJobOperator.created.clear()

    # Building the DAG happens at import time via: dag = salesforce_ingester_dag()
    # We just assert operators were constructed as expected.
    assert (
        _DummyGlueJobOperator.created
    ), "Expected GlueJobOperator to be instantiated."

    glue_op = _DummyGlueJobOperator.created[0]
    script_args = glue_op.kwargs["script_args"]

    # 1) latest zip comes from xcom pull (templated)
    assert (
        script_args["--extra-py-files"]
        == "{{ ti.xcom_pull(task_ids='get_latest_framework_zip') }}"
    )

    # 2) event is a pure Jinja template that pulls the build_event task output
    #    (this is the key fix vs the f-string approach)
    assert (
        script_args["--event"]
        == "{{ ti.xcom_pull(task_ids='build_event_opportunity') }}"
    )

    # sanity: other args exist
    assert script_args["--table"] == "opportunity"
    assert script_args["--vendor"] == "salesforce"
    assert script_args["--env"] == "qa"
    assert script_args["--repo_name"] == "repo"
    assert script_args["--file_path"] == "cfg.yml"
    assert script_args["--github_token"] == "gh-token"
