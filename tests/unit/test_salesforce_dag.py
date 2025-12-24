import importlib

from airflow.models import DagBag


def _import_dag(monkeypatch, workflow_dict, copy_sql):
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

    monkeypatch.setattr(
        "airflow.models.Variable.get",
        lambda k, default_var=None, deserialize_json=False: (
            workflow_dict if "WORKFLOW" in k else copy_sql
        ),
    )

    module = importlib.import_module("dags.salesforce.salesforce_ingester")
    return module.dag


def test_dag_builds_expected_tasks(monkeypatch):
    dag = _import_dag(
        monkeypatch,
        workflow_dict={
            "INGESTER_TABLES": ["Opportunity"],
            "INGESTER_SQL_PARAMS": {},
            "BUCKET_NAME": "test-bucket",
            "REGION": "us-east-1",
        },
        copy_sql="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}'",
    )

    assert dag.dag_id == "debi_ingester_glue_runner"

    assert "latest_framework_zip" in dag.task_ids
    assert "latest_parquet_opportunity" in dag.task_ids
    assert "load_table_opportunity" in dag.task_ids


def test_load_table_uses_s3_uri_param(monkeypatch):
    dag = _import_dag(
        monkeypatch,
        workflow_dict={
            "INGESTER_TABLES": ["Opportunity"],
            "INGESTER_SQL_PARAMS": {},
            "BUCKET_NAME": "test-bucket",
            "REGION": "us-east-1",
        },
        copy_sql="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}'",
    )

    load = dag.get_task("load_table_opportunity")

    assert "params.s3_uri" in load.sql


def test_dependencies(monkeypatch):
    dag = _import_dag(
        monkeypatch,
        workflow_dict={
            "INGESTER_TABLES": ["Opportunity"],
            "INGESTER_SQL_PARAMS": {},
            "BUCKET_NAME": "test-bucket",
            "REGION": "us-east-1",
        },
        copy_sql="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}'",
    )

    framework = dag.get_task("latest_framework_zip")
    parquet = dag.get_task("latest_parquet_opportunity")
    load = dag.get_task("load_table_opportunity")

    assert parquet.task_id in framework.downstream_task_ids
    assert load.task_id in parquet.downstream_task_ids
