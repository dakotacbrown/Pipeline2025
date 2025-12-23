from __future__ import annotations

import importlib
import json
import sys
import types
from typing import Any, Dict

import pytest


def _install_module(
    monkeypatch: pytest.MonkeyPatch, name: str, module: types.ModuleType
) -> None:
    """
    Install/override a module in sys.modules so imports in the DAG module resolve.
    """
    monkeypatch.setitem(sys.modules, name, module)


@pytest.fixture()
def dag_mod(monkeypatch: pytest.MonkeyPatch):
    """
    Import the DAG module with all external deps stubbed (GlueJobOperator, SnowflakeHook, boto3, Variables).
    """

    # ---------------------------------------------------------------------
    # 1) Stub boto3 S3 client used by get_latest_framework_zip
    # ---------------------------------------------------------------------
    class _FakePaginator:
        def paginate(self, Bucket: str, Prefix: str):
            # Two matching zips; newest is the higher LastModified
            return [
                {
                    "Contents": [
                        {
                            "Key": f"{Prefix}debi-etl-framework-glue-0.0.0-PR-1-older.zip",
                            "LastModified": 1,
                        },
                        {
                            "Key": f"{Prefix}debi-etl-framework-glue-0.0.0-PR-2-newest.zip",
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

    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = _fake_boto3_client
    _install_module(monkeypatch, "boto3", boto3_mod)

    # ---------------------------------------------------------------------
    # 2) Stub GlueJobOperator (must be a real BaseOperator subclass or >> breaks)
    # ---------------------------------------------------------------------
    from airflow.models.baseoperator import BaseOperator

    class DummyGlueJobOperator(BaseOperator):
        template_fields = ("script_args",)

        def __init__(self, **kwargs: Any):
            super().__init__(task_id=kwargs["task_id"])
            # Capture whatever the DAG passes in for assertions
            self.kwargs = dict(kwargs)
            self.script_args = kwargs.get("script_args", {})

        def execute(self, context: Any):
            return None

    glue_mod = types.ModuleType("airflow.providers.amazon.aws.operators.glue")
    glue_mod.GlueJobOperator = DummyGlueJobOperator
    _install_module(
        monkeypatch, "airflow.providers.amazon.aws.operators.glue", glue_mod
    )

    # ---------------------------------------------------------------------
    # 3) Stub SnowflakeHook import used by the DAG file
    # ---------------------------------------------------------------------
    class DummySnowflakeHook:
        conn_type = "snowflake"

        def __init__(
            self,
            snowflake_conn_id: str | None = None,
            *args: Any,
            **kwargs: Any,
        ):
            self.snowflake_conn_id = snowflake_conn_id

        def get_conn(self):
            # truthy
            return object()

    snowflake_mod = types.ModuleType(
        "airflow.providers.snowflake.hooks.snowflake"
    )
    snowflake_mod.SnowflakeHook = DummySnowflakeHook
    _install_module(
        monkeypatch,
        "airflow.providers.snowflake.hooks.snowflake",
        snowflake_mod,
    )

    # ---------------------------------------------------------------------
    # 4) Patch Airflow Variables used at *import time*
    # IMPORTANT: Variable.get returns STRINGS in real Airflow unless deserialize_json=True
    # ---------------------------------------------------------------------
    from airflow.models import Variable

    workflow_value = {
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "once",
        "INGESTER_TABLES": json.dumps(["account", "opportunity"]),
        "INGESTER_CONFIG_PATH": "ingester/salesforce.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "C1SCOREDATASERVICES_GITHUB_PASSWORD": "ghp_xxx",
        "INGESTER_START_DATE": "2000-01-01",
        "INGESTER_END_DATE": "2025-12-17",
        "C1SCOREDATASERVICES_EXCHANGE_ID": "ex_id",
        "C1SCOREDATASERVICES_EXCHANGE_SECRET": "ex_secret",
        "CIS_SALESFORCE_USERNAME": "user",
        "CIS_SALESFORCE_PASSWORD": "pass",
        "CIS_SALESFORCE_CLIENTID": "cid",
        "CIS_SALESFORCE_CLIENTSECRET": "csecret",
        "INGESTER_ENV_VARS": json.dumps(
            {"X_UPSTREAM_ENV": "capitalonesoftware-qa"}
        ),
        # optional bits used by your DAG
        "INGESTER_SQL_PARAMS": json.dumps(
            {"DATABASE": "DEV", "SCHEMA": "OPERATIONS"}
        ),
        "JOB_EXECUTION_STATUS_TABLE": "DEV.OPERATIONS.JOB_EXECUTION_STATUS",
        # if your DAG gates load_table on this:
        "INGESTER_COPY_SQL": "copy_template.sql",
    }

    var_map: Dict[str, Any] = {
        # This one MUST be a JSON string (not a dict) because your DAG parses it
        "INGESTER_WORKFLOW_SALESFORCE": json.dumps(workflow_value),
        # Any other Variables accessed directly in the module can be added here if needed
    }

    def fake_get(key: str, default_var: Any = None, **kwargs: Any) -> Any:
        return var_map.get(key, default_var)

    monkeypatch.setattr(Variable, "get", staticmethod(fake_get))

    # ---------------------------------------------------------------------
    # 5) Import the DAG module (after stubbing deps)
    # ---------------------------------------------------------------------
    mod = importlib.import_module("dags.salesforce.salesforce_ingester")
    importlib.reload(mod)
    return mod


def test_safe_task_id(dag_mod):
    assert dag_mod._safe_task_id("Opportunity") == "opportunity"
    assert dag_mod._safe_task_id("__ABC---") == "abc"


def test_safe_json_loads(dag_mod):
    assert dag_mod._safe_json_loads('{"a": 1}', {}) == {"a": 1}
    # python-literal style
    assert dag_mod._safe_json_loads("{'a': 1}", {}) == {"a": 1}
    assert dag_mod._safe_json_loads("", {"x": 1}) == {"x": 1}


def test_get_latest_framework_zip_task_and_callable(dag_mod):
    dag = dag_mod.dag

    # Make sure the task exists in the DAG (TaskFlow task => PythonDecoratedOperator)
    task = dag.get_task("get_latest_framework_zip")
    assert task.task_id == "get_latest_framework_zip"

    # Test underlying logic by calling the python callable directly
    s3_prefix = "s3://c1scoredataservices-qa-east/code/ETL/"
    uri = task.python_callable(s3_prefix=s3_prefix)

    assert isinstance(uri, str)
    assert uri.startswith("s3://c1scoredataservices-qa-east/")
    assert uri.endswith(".zip")
    assert "newest" in uri


def test_dag_builds_and_creates_glue_tasks(dag_mod):
    dag = dag_mod.dag
    # Your @dag(...) sets this; if you create a new dag ad-hoc it’ll be "adhoc_airflow"
    assert dag.dag_id == "debi_ingester_glue_runner"

    for table in ["account", "opportunity"]:
        assert f"build_event_{table}" in dag.task_ids
        assert f"run_glue_job_{table}" in dag.task_ids


def test_glue_operator_event_arg_is_templated_xcom_pull(dag_mod):
    dag = dag_mod.dag
    t = dag.get_task("run_glue_job_account")

    # Our DummyGlueJobOperator stores script_args
    script_args = getattr(t, "script_args", None) or getattr(
        t, "kwargs", {}
    ).get("script_args", {})
    assert "--event" in script_args

    event_arg = script_args["--event"]
    assert isinstance(event_arg, str)
    assert "ti.xcom_pull" in event_arg
    assert "build_event_account" in event_arg
