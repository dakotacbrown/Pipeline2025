from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest


@pytest.fixture
def dag_module(monkeypatch):
    """
    Import the DAG module with all the parse-time dependencies patched so import doesn't
    try to hit real Airflow Variables, Snowflake, boto3, etc.
    """

    # Patch the "dags.common.dag_utilities" functions imported by the module
    # IMPORTANT: patch them on the module they're imported from, not where they come from.
    # We'll patch after import using monkeypatch.setattr on the imported module object.

    # Patch airflow.models.Variable.get before import (most important)
    from airflow.models import Variable

    def fake_variable_get(key, default_var=None, deserialize_json=False):
        # This is the workflow dict variable your code reads:
        if key == "INGESTER_WORKFLOW_SALESFORCE":
            return {
                "INGESTER_GLUE_JOB_NAME": "etl-job",
                "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
                "INGESTER_RUN_MODE": "once",
                "INGESTER_TABLES": ["Account", "Opportunity"],
                "INGESTER_PYTHON_MODULE": "c1-asvc1scoredataservices-common==0.1.41",
                "INGESTER_CONFIG_PATH": "ingester/salesforce.yml",
                "INGESTER_CONFIG_REPO_NAME": "config_management",
                "INGESTER_START_DATE": "2000-01-01",
                "INGESTER_ENV_VARS": {
                    "X_UPSTREAM_ENV": "capitalonesoftware-qa"
                },
                "INGESTER_SQL_PARAMS": {
                    "DATABASE": "validation",
                    "SCHEMA": "public",
                    "JOB_EXECUTION_STATUS_TABLE": "DEV.OPERATIONS.JOB_EXECUTION_STATUS",
                },
            }

        # Make sure copy_sql is present but doesn’t force a real SnowflakeHook.get_conn() in tests
        if key == "INGESTER_COPY_SQL":
            return """
COPY INTO {{ params.target_table }}
FROM '{{ ti.xcom_pull(task_ids=params.glue_task_id)["meta"]["s3_uri"] }}'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
""".strip()

        # Any secrets/credentials → just None
        if key in {
            "C1SCOREDATASERVICES_GITHUB_PASSWORD",
            "C1SCOREDATASERVICES_EXCHANGE_ID",
            "C1SCOREDATASERVICES_EXCHANGE_SECRET",
            "CIS_SALESFORCE_USERNAME",
            "CIS_SALESFORCE_PASSWORD",
            "CIS_SALESFORCE_CLIENTID",
            "CIS_SALESFORCE_CLIENTSECRET",
        }:
            return None

        return default_var

    monkeypatch.setattr(Variable, "get", fake_variable_get, raising=True)

    # Patch SnowflakeHook so import doesn't try to open a real connection
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    monkeypatch.setattr(
        SnowflakeHook, "get_conn", lambda self: SimpleNamespace(), raising=True
    )

    # Now import the DAG module
    mod = importlib.import_module("dags.salesforce.salesforce_ingester")

    # Patch the utility functions that are called at parse time inside the module
    monkeypatch.setattr(
        mod, "get_shairflow_environment", lambda: "qa", raising=False
    )
    monkeypatch.setattr(
        mod, "get_shairflow_region", lambda: "us-east-1", raising=False
    )
    monkeypatch.setattr(
        mod, "get_truncated_shairflow_region", lambda: "use1", raising=False
    )
    monkeypatch.setattr(
        mod,
        "get_bucket_name",
        lambda env, region: "c1scoredataservices-qa-east",
        raising=False,
    )
    monkeypatch.setattr(
        mod,
        "get_cls_oauth_endpoint",
        lambda env: "https://example/token",
        raising=False,
    )
    monkeypatch.setattr(
        mod, "failover_managed_dag_tag", lambda: "failover", raising=False
    )

    return mod


@pytest.fixture
def dag(dag_module):
    # Your file ends with: dag = salesforce_ingester_dag()
    return dag_module.dag


def test_dag_loaded(dag):
    assert dag.dag_id == "debi_ingester_glue_runner"


def test_expected_tasks_exist(dag):
    # 1 shared task + 3 per table
    assert "get_latest_framework_zip" in dag.task_ids

    # tables: Account, Opportunity -> safe ids: account, opportunity
    for safe in ["account", "opportunity"]:
        assert f"build_event_{safe}" in dag.task_ids
        assert f"run_glue_job_{safe}" in dag.task_ids
        assert f"load_table_{safe}" in dag.task_ids

    assert len(dag.task_ids) == 1 + 3 * 2


def test_dependencies_chain(dag):
    for safe in ["account", "opportunity"]:
        latest = dag.get_task("get_latest_framework_zip")
        build = dag.get_task(f"build_event_{safe}")
        run = dag.get_task(f"run_glue_job_{safe}")
        load = dag.get_task(f"load_table_{safe}")

        assert build.task_id in latest.downstream_task_ids
        assert run.task_id in build.downstream_task_ids
        assert load.task_id in run.downstream_task_ids


def test_load_table_params(dag):
    load = dag.get_task("load_table_account")

    # SQLExecuteQueryOperator keeps params on the operator
    params = load.params
    assert params["glue_task_id"] == "run_glue_job_account"
    assert params["target_table"] == "validation.public.Account"


def test_copy_sql_is_string(dag):
    load = dag.get_task("load_table_account")
    assert isinstance(load.sql, str)
    assert "COPY INTO" in load.sql
    # sanity check the templated bit stays templated
    assert "ti.xcom_pull" in load.sql
