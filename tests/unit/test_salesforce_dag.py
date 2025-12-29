import importlib
import io
import types
from unittest.mock import MagicMock

import pytest

DAG_MODULE = "dags.salesforce.salesforce_ingester"


def _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template):
    """
    Import/reload the DAG module with patched Airflow Variables and dag_utilities.
    """
    # Ensure CURRENT_DIR can be computed at import-time
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", "/tmp")

    # Patch dag_utilities functions used at parse time
    fake_utils = types.SimpleNamespace(
        failover_managed_dag_tag=lambda: "failover",
        get_bucket_name=lambda env, trunc_region: "my-bucket",
        get_c1s_oauth_endpoint=lambda env: "https://oauth.example",
        get_shairflow_environment=lambda: "qa",
        get_shairflow_region=lambda: "us-east-1",
        get_truncated_shairflow_region=lambda: "us-east-1",
    )

    monkeypatch.setattr(
        f"{DAG_MODULE}.failover_managed_dag_tag",
        fake_utils.failover_managed_dag_tag,
        raising=False,
    )
    monkeypatch.setattr(
        f"{DAG_MODULE}.get_bucket_name",
        fake_utils.get_bucket_name,
        raising=False,
    )
    monkeypatch.setattr(
        f"{DAG_MODULE}.get_c1s_oauth_endpoint",
        fake_utils.get_c1s_oauth_endpoint,
        raising=False,
    )
    monkeypatch.setattr(
        f"{DAG_MODULE}.get_shairflow_environment",
        fake_utils.get_shairflow_environment,
        raising=False,
    )
    monkeypatch.setattr(
        f"{DAG_MODULE}.get_shairflow_region",
        fake_utils.get_shairflow_region,
        raising=False,
    )
    monkeypatch.setattr(
        f"{DAG_MODULE}.get_truncated_shairflow_region",
        fake_utils.get_truncated_shairflow_region,
        raising=False,
    )

    # Patch Variable.get
    from airflow.models import Variable

    def fake_variable_get(key, default_var=None, deserialize_json=False):
        if key == "C1SCOREDATASERVICES_GITHUB_PASSWORD":
            return "gh-token"
        if key == "C1SCOREDATASERVICES_EXCHANGE_ID":
            return "ex-id"
        if key == "C1SCOREDATASERVICES_EXCHANGE_SECRET":
            return "ex-secret"
        if key == "C1S_SALESFORCE_USERNAME":
            return "sf-user"
        if key == "C1S_SALESFORCE_PASSWORD":
            return "sf-pass"
        if key == "C1S_SALESFORCE_CLIENTID":
            return "sf-client"
        if key == "C1S_SALESFORCE_CLIENTSECRET":
            return "sf-client-secret"

        if key == "INGESTER_WORKFLOW_SALESFORCE":
            if deserialize_json:
                return workflow_var_dict
            return workflow_var_dict

        return default_var

    monkeypatch.setattr(Variable, "get", staticmethod(fake_variable_get))

    # Patch SnowflakeHook.get_conn so parse-time validation passes
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    monkeypatch.setattr(
        SnowflakeHook, "get_conn", lambda self: object(), raising=True
    )

    # Patch boto3 in case get_latest_s3_uri is invoked during anything unexpected
    import boto3

    monkeypatch.setattr(boto3, "client", MagicMock(), raising=True)

    # Patch open() for get_copy_sql (it reads copy/copy_{table}.sql)
    def fake_open(*args, **kwargs):
        return io.StringIO(copy_sql_template)

    monkeypatch.setattr("builtins.open", fake_open, raising=True)

    # Now import/reload module
    mod = importlib.import_module(DAG_MODULE)
    mod = importlib.reload(mod)
    return mod


@pytest.fixture
def workflow_var_dict():
    return {
        "INGESTER_CONFIG_PATH": "ingester/salesforce.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "INGESTER_END_DATE": "2000-01-31",
        "INGESTER_ENV_VARS": {"X_UPSTREAM_ENV": "capitalonesoftware-qa"},
        "INGESTER_TABLES": ["opportunity"],
        "INGESTER_RUN_MODE": "once",
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_SQL_PARAMS": {"DATABASE": "VALIDATION", "SCHEMA": "PUBLIC"},
    }


@pytest.fixture
def copy_sql_template():
    # Template file contents read by get_copy_sql()
    return (
        "DELETE FROM {{ params.target_table }};\n"
        "COPY INTO {{ params.target_table }}\n"
        "FROM @VALIDATION_STAGE/{{ params.s3_uri }}\n"
        "FILE_FORMAT = (TYPE = PARQUET)\n"
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;\n"
        "FORCE = TRUE;\n"
    )


def test_dag_builds_expected_tasks(
    monkeypatch, workflow_var_dict, copy_sql_template
):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    assert dag.dag_id == "debi_ingester_glue_runner"

    # Shared zip task
    assert "latest_framework_zip" in dag.task_ids

    # Table-specific tasks (Opportunity -> safe id "opportunity")
    assert "build_event_opportunity" in dag.task_ids
    assert "run_glue_job_opportunity" in dag.task_ids
    assert "latest_parquet_opportunity" in dag.task_ids
    assert "copy_sql_opportunity" in dag.task_ids
    assert "load_table_opportunity" in dag.task_ids


def test_glue_job_args_reference_xcom(
    monkeypatch, workflow_var_dict, copy_sql_template
):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    run_glue = dag.get_task("run_glue_job_opportunity")
    script_args = run_glue.script_args

    assert (
        script_args["--extra-py-files"]
        == "{{ ti.xcom_pull(task_ids='latest_framework_zip') }}"
    )
    assert (
        script_args["--event"]
        == "{{ ti.xcom_pull(task_ids='build_event_opportunity') }}"
    )


def test_load_table_sql_comes_from_copy_sql_xcom(
    monkeypatch, workflow_var_dict, copy_sql_template
):
    """
    load_table.sql should be an XComArg pointing at copy_sql_<table>,
    not params["s3_uri"] like the old implementation.
    """
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    load = dag.get_task("load_table_opportunity")

    from airflow.models.xcom_arg import XComArg

    assert isinstance(load.sql, XComArg)
    # XComArg.operator is the upstream task object (copy_sql_opportunity)
    assert load.sql.operator.task_id == "copy_sql_opportunity"


def test_dependencies(monkeypatch, workflow_var_dict, copy_sql_template):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    latest_zip = dag.get_task("latest_framework_zip")
    build_event = dag.get_task("build_event_opportunity")
    run_glue = dag.get_task("run_glue_job_opportunity")
    latest_parquet = dag.get_task("latest_parquet_opportunity")
    copy_sql = dag.get_task("copy_sql_opportunity")
    load = dag.get_task("load_table_opportunity")

    # latest_zip -> build_event -> run_glue
    assert build_event.task_id in latest_zip.downstream_task_ids
    assert run_glue.task_id in build_event.downstream_task_ids

    # run_glue -> latest_parquet -> copy_sql -> load_table
    assert latest_parquet.task_id in run_glue.downstream_task_ids
    assert copy_sql.task_id in latest_parquet.downstream_task_ids
    assert load.task_id in copy_sql.downstream_task_ids
