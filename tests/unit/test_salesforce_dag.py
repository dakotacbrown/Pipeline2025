import importlib
import types

import pytest

DAG_MODULE = "dags.salesforce.salesforce_ingester"


def _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template):
    """
    Import/reload the DAG module with patched Airflow Variables and dag_utilities.
    """
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
        if key == "INGESTER_COPY_SQL":
            return copy_sql_template
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
    # Use s3_uri (our DAG sets both s3_uri and s3_prefix anyway)
    return (
        "COPY INTO {{ params.target_table }}\n"
        "FROM '{{ params.s3_uri }}'\n"
        "FILE_FORMAT = (TYPE = PARQUET)\n"
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;\n"
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


def test_load_table_sql_uses_latest_parquet_xcom(
    monkeypatch, workflow_var_dict, copy_sql_template
):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    load = dag.get_task("load_table_opportunity")

    # params injected into SQLExecuteQueryOperator
    assert load.params["target_table"] == "VALIDATION.PUBLIC.opportunity"
    assert (
        load.params["s3_uri"]
        == "{{ ti.xcom_pull(task_ids='latest_parquet_opportunity') }}"
    )
    # backward-compat alias
    assert (
        load.params["s3_prefix"]
        == "{{ ti.xcom_pull(task_ids='latest_parquet_opportunity') }}"
    )


def test_dependencies(monkeypatch, workflow_var_dict, copy_sql_template):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    latest_zip = dag.get_task("latest_framework_zip")
    build_event = dag.get_task("build_event_opportunity")
    run_glue = dag.get_task("run_glue_job_opportunity")
    latest_parquet = dag.get_task("latest_parquet_opportunity")
    load = dag.get_task("load_table_opportunity")

    # latest_zip -> build_event -> run_glue
    assert build_event.task_id in latest_zip.downstream_task_ids
    assert run_glue.task_id in build_event.downstream_task_ids

    # load waits for both latest_parquet and run_glue
    assert load.task_id in latest_parquet.downstream_task_ids
    assert load.task_id in run_glue.downstream_task_ids
