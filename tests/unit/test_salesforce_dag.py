from __future__ import annotations

import importlib
import sys
import types
from typing import Any, Dict, List

import pytest


def _install_module(
    monkeypatch: pytest.MonkeyPatch, name: str, module: types.ModuleType
) -> None:
    """
    Install a module into sys.modules so `import x.y.z` works during DAG import.
    Also ensures parent packages exist (x, x.y).
    """
    parts = name.split(".")
    for i in range(1, len(parts)):
        pkg = ".".join(parts[:i])
        if pkg not in sys.modules:
            sys.modules[pkg] = types.ModuleType(pkg)
    monkeypatch.setitem(sys.modules, name, module)


@pytest.fixture()
def dag_mod(monkeypatch: pytest.MonkeyPatch):
    """
    Import dags.salesforce.salesforce_ingester with all provider + internal deps stubbed.
    Returns the imported module.
    """
    # -------------------------
    # 1) Stub boto3 (for get_latest_framework_zip python_callable testing)
    # -------------------------
    boto3_mod = types.ModuleType("boto3")

    class _FakePaginator:
        def paginate(self, Bucket: str, Prefix: str):
            # Two objects: newer has larger LastModified
            return [
                {
                    "Contents": [
                        {
                            "Key": f"{Prefix}debi-etl-framework-glue-0.0.0-PullRequest1.PR-1-older.zip",
                            "LastModified": 1,
                        },
                        {
                            "Key": f"{Prefix}debi-etl-framework-glue-0.0.0-PullRequest1.PR-2-newer.zip",
                            "LastModified": 2,
                        },
                    ]
                }
            ]

    class _FakeS3Client:
        def get_paginator(self, name: str):
            assert name == "list_objects_v2"
            return _FakePaginator()

    def _fake_boto3_client(service: str, *args: Any, **kwargs: Any):
        assert service == "s3"
        return _FakeS3Client()

    boto3_mod.client = _fake_boto3_client
    _install_module(monkeypatch, "boto3", boto3_mod)

    # -------------------------
    # 2) Stub GlueJobOperator (must be a real BaseOperator so dependencies work)
    # -------------------------
    glue_mod = types.ModuleType("airflow.providers.amazon.aws.operators.glue")

    from airflow.models.baseoperator import BaseOperator

    class DummyGlueJobOperator(BaseOperator):
        template_fields = ("script_args",)

        def __init__(self, **kwargs: Any):
            self.job_name = kwargs.get("job_name")
            self.aws_conn_id = kwargs.get("aws_conn_id")
            self.region_name = kwargs.get("region_name")
            self.script_args = kwargs.get("script_args", {})
            self.wait_for_completion = kwargs.get("wait_for_completion", False)
            super().__init__(task_id=kwargs["task_id"])

        def execute(self, context: Any):
            return None

    glue_mod.GlueJobOperator = DummyGlueJobOperator
    _install_module(
        monkeypatch, "airflow.providers.amazon.aws.operators.glue", glue_mod
    )

    # -------------------------
    # 3) Stub SnowflakeHook (import-time print references conn_type)
    # -------------------------
    snowflake_mod = types.ModuleType(
        "airflow.providers.snowflake.hooks.snowflake"
    )

    class DummySnowflakeHook:
        conn_type = "snowflake"

        def __init__(self, *args: Any, **kwargs: Any):
            pass

        def get_conn(self):
            # called only if copy_sql is set; we keep copy_sql None in tests
            return object()

    snowflake_mod.SnowflakeHook = DummySnowflakeHook
    _install_module(
        monkeypatch,
        "airflow.providers.snowflake.hooks.snowflake",
        snowflake_mod,
    )

    # -------------------------
    # 4) Stub internal deps from dags.common.*
    # -------------------------
    dag_utils_mod = types.ModuleType("dags.common.dag_utilities")

    from airflow.operators.empty import EmptyOperator
    from airflow.utils.task_group import TaskGroup

    def failover_managed_dag_tag() -> str:
        return "failover-managed-dag"

    def get_bucket_name(env: str, truncated_region: str) -> str:
        return "c1scoredataservices-qa-east"

    def get_cls_oauth_endpoint(env: str) -> str:
        return "https://example/oauth"

    def get_shairflow_environment() -> str:
        return "qa"

    def get_shairflow_region() -> str:
        return "east"

    def get_truncated_shairflow_region() -> str:
        return "ea"

    def successful_execution_status(**kwargs: Any) -> Dict[str, Any]:
        return kwargs

    def sql_transformation_task_group(
        task_group_id: str,
        **kwargs: Any,
    ):
        # Return a REAL TaskGroup so `run_glue >> load_table` works.
        with TaskGroup(group_id=task_group_id) as tg:
            EmptyOperator(task_id="sql")
        return tg

    dag_utils_mod.failover_managed_dag_tag = failover_managed_dag_tag
    dag_utils_mod.get_bucket_name = get_bucket_name
    dag_utils_mod.get_cls_oauth_endpoint = get_cls_oauth_endpoint
    dag_utils_mod.get_shairflow_environment = get_shairflow_environment
    dag_utils_mod.get_shairflow_region = get_shairflow_region
    dag_utils_mod.get_truncated_shairflow_region = (
        get_truncated_shairflow_region
    )
    dag_utils_mod.sql_transformation_task_group = sql_transformation_task_group
    dag_utils_mod.successful_execution_status = successful_execution_status
    _install_module(monkeypatch, "dags.common.dag_utilities", dag_utils_mod)

    slack_mod = types.ModuleType("dags.common.slack")
    slack_mod.task_fail_slack_alert = lambda *a, **k: None
    _install_module(monkeypatch, "dags.common.slack", slack_mod)

    udf_mod = types.ModuleType("dags.common.user_defined_filters")
    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = lambda *a, **k: ""
    _install_module(monkeypatch, "dags.common.user_defined_filters", udf_mod)

    # -------------------------
    # 5) Patch Variable.get to return dict for workflow, plus other vars
    # -------------------------
    from airflow.models import Variable as AirflowVariable

    workflow_dict = {
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "once",
        "INGESTER_TABLES": ["Account", "Opportunity"],
        "INGESTER_PYTHON_MODULE": "c1-asvc1scoredataservices-common==0.1.41",
        "INGESTER_START_DATE": "2000-01-01",
        "INGESTER_END_DATE": "2025-12-17",
        "INGESTER_CONFIG_PATH": "ingester/salesforce.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "INGESTER_ENV_VARS": {"X_UPSTREAM_ENV": "capitalonesoftware-qa"},
        "INGESTER_SQL_PARAMS": {
            "DATABASE": "validation",
            "SCHEMA": "public",
            "JOB_EXECUTION_STATUS_TABLE": "DEV.OPERATIONS.JOB_EXECUTION_STATUS",
        },
    }

    def fake_variable_get(
        key: str,
        default_var: Any = None,
        deserialize_json: bool = False,
    ):
        if key == "INGESTER_WORKFLOW_SALESFORCE":
            # Your DAG now asks for deserialize_json=True and expects a dict.
            return workflow_dict
        # Defaults for other Variables referenced at import-time
        if key == "C1SCOREDATASERVICES_GITHUB_PASSWORD":
            return "ghp_xxx"
        if key == "C1SCOREDATASERVICES_EXCHANGE_ID":
            return "ex_id"
        if key == "C1SCOREDATASERVICES_EXCHANGE_SECRET":
            return "ex_secret"
        if key == "C1S_SALESFORCE_USERNAME":
            return "user"
        if key == "C1S_SALESFORCE_PASSWORD":
            return "pass"
        if key == "C1S_SALESFORCE_CLIENTID":
            return "cid"
        if key == "C1S_SALESFORCE_CLIENTSECRET":
            return "csecret"
        if key == "INGESTER_COPY_SQL":
            return None
        return default_var

    monkeypatch.setattr(AirflowVariable, "get", staticmethod(fake_variable_get))

    # -------------------------
    # 6) Import the DAG module
    # -------------------------
    mod = importlib.import_module("dags.salesforce.salesforce_ingester")
    importlib.reload(mod)
    return mod


def test_safe_task_id(dag_mod):
    assert dag_mod._safe_task_id("Opportunity") == "opportunity"
    assert dag_mod._safe_task_id("__ABC---") == "abc"


def test_get_latest_framework_zip_python_callable_returns_newest(dag_mod):
    """
    Call the underlying python callable (not the XComArg) to validate selection logic.
    """
    dag = dag_mod.dag
    op = dag.get_task("get_latest_framework_zip")
    # PythonDecoratedOperator exposes python_callable
    uri = op.python_callable(
        s3_prefix="s3://c1scoredataservices-qa-east/code/ETL/"
    )
    assert uri.startswith("s3://c1scoredataservices-qa-east/code/ETL/")
    assert uri.endswith(".zip")
    assert "older" not in uri
    assert "newer" in uri


def test_dag_builds_and_creates_expected_tasks(dag_mod):
    dag = dag_mod.dag
    assert dag is not None
    assert dag.dag_id == "debi_ingester_glue_runner"

    # Core task always present
    assert "get_latest_framework_zip" in dag.task_ids

    # Table-driven tasks
    for safe in ["account", "opportunity"]:
        assert f"build_event_{safe}" in dag.task_ids
        assert f"run_glue_job_{safe}" in dag.task_ids
        # TaskGroup exists
        assert f"load_{safe}_table" in dag.task_group.children


def test_glue_operator_event_arg_is_templated_xcom_pull(dag_mod):
    dag = dag_mod.dag

    t = dag.get_task("run_glue_job_account")
    assert "--extra-py-files" in t.script_args
    assert (
        t.script_args["--extra-py-files"]
        == "{{ ti.xcom_pull(task_ids='get_latest_framework_zip') }}"
    )
    assert (
        t.script_args["--event"]
        == "{{ ti.xcom_pull(task_ids='build_event_account') }}"
    )

    # sanity: python modules installer option present
    assert "--python-modules-installer-option" in t.script_args
    assert "index-url" in t.script_args["--python-modules-installer-option"]


def test_glue_operator_has_expected_fixed_args(dag_mod):
    dag = dag_mod.dag
    t = dag.get_task("run_glue_job_opportunity")

    assert t.job_name == "etl-job"
    assert t.aws_conn_id == "etl-net-conn"
    assert t.region_name == "east"  # from stubbed get_shairflow_region()

    # These come directly from workflow_dict
    assert t.script_args["--env"] == "qa"
    assert t.script_args["--run_mode"] == "once"
    assert t.script_args["--vendor"] == "salesforce"
    assert t.script_args["--repo_name"] == "config_management"
    assert t.script_args["--file_path"] == "ingester/salesforce.yml"
