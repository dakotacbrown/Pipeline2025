from __future__ import annotations

import importlib
import json
import sys
import types
from typing import Any, Dict, List

import pytest

DAG_IMPORT = "dags.salesforce.salesforce_ingester"


def _install_module(
    monkeypatch: pytest.MonkeyPatch, name: str, module: types.ModuleType
) -> None:
    """
    Register a module (and any missing parent packages) into sys.modules so imports work.
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
    Import the Salesforce ingester DAG module with all external deps stubbed.

    Key design:
    - Keep Variable.get patching (your preference).
    - Ensure Variable.get("INGESTER_WORKFLOW_SALESFORCE", "{}") returns a *dict*.
    - Stub boto3, GlueJobOperator, SnowflakeHook, dag_utilities at import time.
    """

    # --------------------------
    # 1) Stub boto3 (list S3 zips)
    # --------------------------
    boto3_mod = types.ModuleType("boto3")

    class _FakePaginator:
        def paginate(self, Bucket: str, Prefix: str):
            # Provide some keys that should match your pattern "debi-etl-framework-glue*.zip"
            yield {
                "Contents": [
                    {
                        "Key": f"{Prefix}debi-etl-framework-glue-0.0.0-PullRequest1.PR-1-abc.zip",
                        "LastModified": 2,
                    },
                    {
                        "Key": f"{Prefix}debi-etl-framework-glue-0.0.0-PullRequest1.PR-1-older.zip",
                        "LastModified": 1,
                    },
                ]
            }

    class _FakeS3Client:
        def get_paginator(self, name: str):
            assert name == "list_objects_v2"
            return _FakePaginator()

    def _fake_boto3_client(service: str, *args: Any, **kwargs: Any):
        assert service == "s3"
        return _FakeS3Client()

    boto3_mod.client = _fake_boto3_client
    _install_module(monkeypatch, "boto3", boto3_mod)

    # --------------------------
    # 2) Stub GlueJobOperator
    # --------------------------
    glue_mod = types.ModuleType("airflow.providers.amazon.aws.operators.glue")

    class DummyGlueJobOperator:
        def __init__(self, **kwargs: Any):
            # Keep what we need for asserts
            self.kwargs = kwargs
            self.task_id = kwargs.get("task_id")

        def __rshift__(self, other):
            return other

        def __lshift__(self, other):
            return other

    glue_mod.GlueJobOperator = DummyGlueJobOperator
    _install_module(
        monkeypatch, "airflow.providers.amazon.aws.operators.glue", glue_mod
    )

    # --------------------------
    # 3) Stub SnowflakeHook
    # --------------------------
    snowflake_mod = types.ModuleType(
        "airflow.providers.snowflake.hooks.snowflake"
    )

    class DummySnowflakeHook:
        conn_type = "snowflake"

        def __init__(self, snowflake_conn_id: str):
            self.snowflake_conn_id = snowflake_conn_id

        def get_conn(self):
            # Return a truthy value so "if not hook.get_conn(): raise" doesn't fire
            return object()

    snowflake_mod.SnowflakeHook = DummySnowflakeHook
    _install_module(
        monkeypatch,
        "airflow.providers.snowflake.hooks.snowflake",
        snowflake_mod,
    )

    # --------------------------
    # 4) Stub dags.common.dag_utilities
    # --------------------------
    dag_utils = types.ModuleType("dags.common.dag_utilities")

    def failover_managed_dag_tag() -> str:
        return "failover-managed"

    def get_bucket_name(env: str, truncated_region: str) -> str:
        return "c1scoredataservices-qa-east"

    def get_c1s_oauth_endpoint(env: str) -> str:
        return f"https://oauth/{env}"

    def get_shairflow_environment() -> str:
        return "qa"

    def get_shairflow_region() -> str:
        return "us-east-1"

    def get_truncated_shairflow_region() -> str:
        return "east"

    def successful_execution_status(**kwargs: Any) -> Dict[str, Any]:
        return {"status": "ok", **kwargs}

    # This is likely your Snowflake SQL task group builder.
    # We just need a dummy object that can chain with >>.
    class DummyTaskGroup:
        def __init__(self, group_id: str):
            self.group_id = group_id

        def __rshift__(self, other):
            return other

        def __lshift__(self, other):
            return other

    def sql_transformation_task_group(**kwargs: Any):
        group_id = kwargs.get("task_group_id", "load_table")
        return DummyTaskGroup(group_id=group_id)

    dag_utils.failover_managed_dag_tag = failover_managed_dag_tag
    dag_utils.get_bucket_name = get_bucket_name
    dag_utils.get_c1s_oauth_endpoint = get_c1s_oauth_endpoint
    dag_utils.get_shairflow_environment = get_shairflow_environment
    dag_utils.get_shairflow_region = get_shairflow_region
    dag_utils.get_truncated_shairflow_region = get_truncated_shairflow_region
    dag_utils.sql_transformation_task_group = sql_transformation_task_group
    dag_utils.successful_execution_status = successful_execution_status

    _install_module(monkeypatch, "dags.common.dag_utilities", dag_utils)

    # --------------------------
    # 5) Stub slack alert import (if present)
    # --------------------------
    slack_mod = types.ModuleType("dags.common.slack")
    slack_mod.task_fail_slack_alert = lambda *a, **k: None
    _install_module(monkeypatch, "dags.common.slack", slack_mod)

    # --------------------------
    # 6) Stub user_defined_filters import (if present)
    # --------------------------
    udf_mod = types.ModuleType("dags.common.user_defined_filters")
    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = lambda s: s
    _install_module(monkeypatch, "dags.common.user_defined_filters", udf_mod)

    # --------------------------
    # 7) Patch Airflow Variable.get (your preferred approach)
    # --------------------------
    from airflow.models import Variable

    workflow_salesforce = {
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "once",
        "INGESTER_TABLES": json.dumps(["Account", "Opportunity History"]),
        "INGESTER_CONFIG_PATH": "ingester/salesforce.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "INGESTER_START_DATE": "2000-01-01",
        "INGESTER_END_DATE": "2025-12-17",
        "INGESTER_ENV_VARS": json.dumps(
            {"X_UPSTREAM_ENV": "capitalonesoftware-qa"}
        ),
        "INGESTER_PYTHON_MODULE": "c1-asvc1scoredataservices-common==0.1.41",
    }

    # IMPORTANT: keys read via Variable.get("<KEY>") must be TOP-LEVEL
    var_map: Dict[str, Any] = {
        "INGESTER_WORKFLOW_SALESFORCE": workflow_salesforce,
        "C1SCOREDATASERVICES_GITHUB_PASSWORD": "ghp-xxx",
        "C1SCOREDATASERVICES_EXCHANGE_ID": "ex_id",
        "C1SCOREDATASERVICES_EXCHANGE_SECRET": "ex_secret",
        "C1S_SALESFORCE_USERNAME": "user",
        "C1S_SALESFORCE_PASSWORD": "pass",
        "C1S_SALESFORCE_CLIENTID": "cid",
        "C1S_SALESFORCE_CLIENTSECRET": "csecret",
        # Turn on the Snowflake load path in your DAG
        "INGESTER_COPY_SQL": "copy.sql",
        # Optional if used by generate_params in DAG
        "INGESTER_SQL_PARAMS": json.dumps(
            {
                "DATABASE": "VALIDATION",
                "SCHEMA": "PUBLIC",
                "JOB_EXECUTION_STATUS_TABLE": "DEV.OPERATIONS.JOB_EXECUTION_STATUS",
            }
        ),
    }

    def fake_get(key: str, default_var: Any = None, **kwargs: Any) -> Any:
        return var_map.get(key, default_var)

    monkeypatch.setattr(Variable, "get", staticmethod(fake_get))

    # --------------------------
    # 8) Finally import + reload the DAG module
    # --------------------------
    mod = importlib.import_module(DAG_IMPORT)
    importlib.reload(mod)
    return mod


def test_safe_task_id(dag_mod):
    assert dag_mod._safe_task_id("Opportunity History") == "opportunity_history"
    assert dag_mod._safe_task_id("___ABC---") == "abc"


def test_safe_json_loads(dag_mod):
    assert dag_mod._safe_json_loads('{"a": 1}', {}) == {"a": 1}
    # fallback ast.literal_eval path (single quotes)
    assert dag_mod._safe_json_loads("{'a': 1}", {}) == {"a": 1}
    assert dag_mod._safe_json_loads("", {"x": 1}) == {"x": 1}


def test_get_latest_framework_zip_returns_newest(dag_mod):
    # Uses stubbed boto3 paginator above
    s3_prefix = "s3://c1scoredataservices-qa-east/code/ETL/"
    uri = dag_mod.get_latest_framework_zip(s3_prefix=s3_prefix)
    assert uri.startswith("s3://c1scoredataservices-qa-east/")
    assert uri.endswith(".zip")
    assert "older" not in uri  # newest chosen by LastModified


def test_dag_builds_and_creates_glue_tasks(dag_mod):
    dag = dag_mod.dag
    assert dag is not None

    # tables in our Variable patch: Account, Opportunity History
    expected_tables: List[str] = ["account", "opportunity_history"]

    # tasks created in for-loop:
    # - build_event_<safe>
    # - run_glue_job_<safe>
    for safe in expected_tables:
        assert f"build_event_{safe}" in dag.task_ids
        assert f"run_glue_job_{safe}" in dag.task_ids


def test_glue_operator_receives_event_as_xcom_template_string(dag_mod):
    dag = dag_mod.dag

    # pick one task
    t = dag.get_task("run_glue_job_account")
    script_args = t.kwargs["script_args"]

    # the event arg should be a *templated string* pulling the build_event task output
    assert "--event" in script_args
    assert "{{" in script_args["--event"]
    assert "ti.xcom_pull" in script_args["--event"]
    assert "build_event_account" in script_args["--event"]


def test_sql_task_group_created_when_copy_sql_present(dag_mod):
    dag = dag_mod.dag

    # DAG creates a task group per table with id like "load_<table>_table"
    assert "load_account_table" in dag.task_group_dict
    assert (
        "load_opportunity history_table".replace(" ", "_")
        not in dag.task_group_dict
    )  # sanity
