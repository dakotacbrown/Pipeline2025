import importlib
import sys


def _patch_common(monkeypatch):
    # Patch the modules your DAG imports-from BEFORE importing the DAG module.
    import dags.common.dag_utilities as du
    import dags.common.slack as slack
    import dags.common.user_defined_filters as udf

    monkeypatch.setattr(du, "get_shairflow_environment", lambda: "qa")
    monkeypatch.setattr(du, "get_shairflow_region", lambda: "us-east-1")
    monkeypatch.setattr(du, "get_truncated_shairflow_region", lambda: "qa-east")
    monkeypatch.setattr(du, "get_bucket_name", lambda env, trunc: "test-bucket")
    monkeypatch.setattr(
        du, "get_c1s_oauth_endpoint", lambda env: "https://oauth.example/token"
    )
    monkeypatch.setattr(du, "failover_managed_dag_tag", lambda: "failover-tag")

    monkeypatch.setattr(slack, "task_fail_slack_alert", lambda *a, **k: None)
    monkeypatch.setattr(udf, "ts_nodash_to_YYYYMMDDHHmmss", lambda s: s)


def _import_dag(monkeypatch, workflow_dict, copy_sql):
    _patch_common(monkeypatch)

    # Patch Variable.get used inside the DAG factory
    from airflow.models import Variable

    def _fake_variable_get(key, default_var=None, deserialize_json=False):
        if key == "INGESTER_COPY_SQL":
            return copy_sql
        if key.startswith("INGESTER_WORKFLOW_SALESFORCE"):
            return workflow_dict
        return default_var

    monkeypatch.setattr(Variable, "get", _fake_variable_get)

    # Ensure a clean import if you re-run tests in same session
    mod_name = "dags.salesforce.salesforce_ingester"
    if mod_name in sys.modules:
        del sys.modules[mod_name]

    module = importlib.import_module(mod_name)
    return module.dag


def test_dag_builds_expected_tasks(monkeypatch):
    dag = _import_dag(
        monkeypatch,
        workflow_dict={
            "INGESTER_TABLES": ["Opportunity"],
            "INGESTER_SQL_PARAMS": {},
            "INGESTER_GLUE_JOB_NAME": "etl-job",
            "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        },
        copy_sql="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}'",
    )

    assert dag.dag_id == "debi_ingester_glue_runner"

    # Shared zip task
    assert "latest_framework_zip" in dag.task_ids

    # Per-table tasks
    assert "build_event_opportunity" in dag.task_ids
    assert "run_glue_job_opportunity" in dag.task_ids
    assert "latest_parquet_opportunity" in dag.task_ids
    assert "load_table_opportunity" in dag.task_ids


def test_glue_operator_templates(monkeypatch):
    dag = _import_dag(
        monkeypatch,
        workflow_dict={
            "INGESTER_TABLES": ["Opportunity"],
        },
        copy_sql="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}'",
    )

    run_glue = dag.get_task("run_glue_job_opportunity")

    # Ensure these are templated pulls, not evaluated at parse time
    assert (
        run_glue.script_args["--extra-py-files"]
        == "{{ ti.xcom_pull(task_ids='latest_framework_zip') }}"
    )
    assert (
        "ti.xcom_pull(task_ids='build_event_opportunity')"
        in run_glue.script_args["--event"]
    )


def test_load_table_uses_latest_parquet_xcom(monkeypatch):
    dag = _import_dag(
        monkeypatch,
        workflow_dict={
            "INGESTER_TABLES": ["Opportunity"],
            "INGESTER_SQL_PARAMS": {"DATABASE": "DEV", "SCHEMA": "OPERATIONS"},
        },
        copy_sql="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}'",
    )

    load = dag.get_task("load_table_opportunity")

    # SQL string references params.s3_uri
    assert "params.s3_uri" in load.sql

    # Params contain templated XCom pull for latest parquet task
    assert load.params["target_table"] == "DEV.OPERATIONS.Opportunity"
    assert (
        "ti.xcom_pull(task_ids='latest_parquet_opportunity')"
        in load.params["s3_uri"]
    )


def test_dependencies(monkeypatch):
    dag = _import_dag(
        monkeypatch,
        workflow_dict={"INGESTER_TABLES": ["Opportunity"]},
        copy_sql="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}'",
    )

    latest_zip = dag.get_task("latest_framework_zip")
    build_event = dag.get_task("build_event_opportunity")
    run_glue = dag.get_task("run_glue_job_opportunity")
    latest_parquet = dag.get_task("latest_parquet_opportunity")
    load_table = dag.get_task("load_table_opportunity")

    # build_event -> run_glue
    assert run_glue.task_id in build_event.downstream_task_ids

    # latest_zip -> run_glue
    assert run_glue.task_id in latest_zip.downstream_task_ids

    # latest_parquet -> load_table
    assert load_table.task_id in latest_parquet.downstream_task_ids

    # run_glue -> load_table
    assert load_table.task_id in run_glue.downstream_task_ids
