from __future__ import annotations

import importlib
import json
import sys
import types
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

DAG_MODULE = "dags.salesforce.salesforce_ingester"


def _install_module(name: str, module: types.ModuleType) -> None:
    parts = name.split(".")
    for i in range(1, len(parts)):
        pkg = ".".join(parts[:i])
        if pkg not in sys.modules:
            m = types.ModuleType(pkg)
            m.__path__ = []  # mark as package
            sys.modules[pkg] = m
    sys.modules[name] = module


@pytest.fixture()
def dag_mod(monkeypatch):
    # ---- stub internal imports used by the DAG ----
    dag_utils = types.ModuleType("dags.common.dag_utilities")
    dag_utils.failover_managed_dag_tag = lambda: "failover-managed"
    dag_utils.get_shairflow_environment = lambda: "qa"
    dag_utils.get_shairflow_region = lambda: "us-east-1"
    dag_utils.get_truncated_shairflow_region = lambda: "use1"
    dag_utils.get_bucket_name = lambda env, region: "my-bucket"
    dag_utils.get_c1s_oauth_endpoint = (
        lambda env: "https://example.invalid/oauth"
    )
    _install_module("dags.common.dag_utilities", dag_utils)

    slack_mod = types.ModuleType("dags.common.slack")
    slack_mod.task_fail_slack_alert = lambda *a, **k: None
    _install_module("dags.common.slack", slack_mod)

    udf_mod = types.ModuleType("dags.common.user_defined_filters")
    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = lambda s: s
    _install_module("dags.common.user_defined_filters", udf_mod)

    # ---- stub GlueJobOperator import path (so we can inspect script_args) ----
    from airflow.models.baseoperator import BaseOperator

    class StubGlueJobOperator(BaseOperator):
        template_fields = ("script_args",)

        def __init__(
            self,
            *,
            job_name: str,
            aws_conn_id: str | None = None,
            region_name: str | None = None,
            script_args: Dict[str, Any] | None = None,
            wait_for_completion: bool | None = None,
            **kwargs: Any,
        ) -> None:
            super().__init__(**kwargs)
            self.job_name = job_name
            self.aws_conn_id = aws_conn_id
            self.region_name = region_name
            self.script_args = script_args or {}
            self.wait_for_completion = wait_for_completion

    glue_mod = types.ModuleType("airflow.providers.amazon.aws.operators.glue")
    glue_mod.GlueJobOperator = StubGlueJobOperator
    _install_module("airflow.providers.amazon.aws.operators.glue", glue_mod)

    # ---- patch Airflow Variables used at parse time ----
    from airflow.models import Variable

    var_map = {
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "once",
        "INGESTER_TABLES": json.dumps(["Account", "Opportunity History"]),
        "INGESTER_VENDOR": "salesforce",
        "INGESTER_CONFIG_PATH": "ingester/salesforce.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "C1SCOREDATASERVICES_GITHUB_PASSWORD": "ghp_xxx",
        "INGESTER_START_DATE": "2000-01-01",
        "INGESTER_END_DATE": "2025-12-17",
        "C1SCOREDATASERVICES_EXCHANGE_ID": "ex_id",
        "C1SCOREDATASERVICES_EXCHANGE_SECRET": "ex_secret",
        "C1S_SALESFORCE_USERNAME": "user",
        "C1S_SALESFORCE_PASSWORD": "pass",
        "C1S_SALESFORCE_CLIENTID": "cid",
        "C1S_SALESFORCE_CLIENTSECRET": "csecret",
        "INGESTER_ENV_VARS": json.dumps(
            {"qa": {"X_UPSTREAM_ENV": "capitalonesoftware-qa"}}
        ),
    }

    def fake_get(key: str, default_var: Any = None, **kwargs: Any) -> Any:
        return var_map.get(key, default_var)

    monkeypatch.setattr(Variable, "get", staticmethod(fake_get))

    # ---- stub boto3 client used by get_latest_framework_zip ----
    class FakePaginator:
        def paginate(self, Bucket: str, Prefix: str):
            assert Bucket == "my-bucket"
            assert Prefix == "code/ETL/"
            return [
                {
                    "Contents": [
                        {
                            "Key": "code/ETL/debi-etl-framework-glue-1.0.1.zip",
                            "LastModified": datetime(
                                2025, 1, 1, tzinfo=timezone.utc
                            ),
                        },
                        {
                            "Key": "code/ETL/debi-etl-framework-glue-1.0.2.zip",
                            "LastModified": datetime(
                                2025, 2, 1, tzinfo=timezone.utc
                            ),
                        },
                        # non-matching file should be ignored
                        {
                            "Key": "code/ETL/other.zip",
                            "LastModified": datetime(
                                2025, 3, 1, tzinfo=timezone.utc
                            ),
                        },
                    ]
                }
            ]

    class FakeS3:
        def get_paginator(self, name: str):
            assert name == "list_objects_v2"
            return FakePaginator()

    import boto3

    monkeypatch.setattr(boto3, "client", lambda service: FakeS3())

    # ---- import/reload module under test ----
    if DAG_MODULE in sys.modules:
        mod = importlib.reload(sys.modules[DAG_MODULE])
    else:
        mod = importlib.import_module(DAG_MODULE)
    return mod


def test_dag_metadata(dag_mod):
    dag = dag_mod.dag
    assert dag.dag_id == "debi_ingester_glue_runner"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert "airflow-2.x.x-compatible" in (dag.tags or [])


def test_tasks_exist_for_tables(dag_mod):
    dag = dag_mod.dag
    ids = set(dag.task_ids)

    assert "get_latest_framework_zip" in ids

    assert "build_event_account" in ids
    assert "run_glue_job_account" in ids

    assert "build_event_opportunity_history" in ids
    assert "run_glue_job_opportunity_history" in ids


def test_dependencies_latest_zip_to_build_to_glue(dag_mod):
    dag = dag_mod.dag

    build = dag.get_task("build_event_account")
    glue = dag.get_task("run_glue_job_account")

    assert "get_latest_framework_zip" in build.upstream_task_ids
    assert build.task_id in glue.upstream_task_ids


def test_glue_script_args_include_single_extra_py_files_xcom(dag_mod):
    dag = dag_mod.dag
    glue = dag.get_task("run_glue_job_account")

    assert (
        glue.script_args["--extra-py-files"]
        == "{{ ti.xcom_pull(task_ids='get_latest_framework_zip') }}"
    )
    assert glue.script_args["--table"] == "Account"
    assert "--event" in glue.script_args
    assert "build_event__account" in glue.script_args["--event"]


def test_get_latest_framework_zip_callable_returns_newest(dag_mod):
    dag = dag_mod.dag
    t = dag.get_task("get_latest_framework_zip")

    # Airflow TaskFlow task is backed by an operator that has python_callable
    out = t.python_callable("s3://my-bucket/code/ETL/")
    assert out == "s3://my-bucket/code/ETL/debi-etl-framework-glue-1.0.2.zip"
