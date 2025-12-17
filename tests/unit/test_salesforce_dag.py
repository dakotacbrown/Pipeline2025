import os
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator


@pytest.fixture()
def dagbag(monkeypatch) -> DagBag:
    """
    Airflow Variables are read at DAG-parse time in your DAG definition,
    so set AIRFLOW_VAR_* before DagBag loads the file.
    """
    monkeypatch.setenv("AIRFLOW_VAR_DEBI_INGESTOR_GLUE_JOB_NAME", "unit-test-glue-job")
    monkeypatch.setenv("AIRFLOW_VAR_GITHUB_TOKEN", "unit-test-gh-token")
    monkeypatch.setenv("AIRFLOW_VAR_AWS_REGION", "us-east-1")

    dags_folder = Path(__file__).resolve().parents[1] / "dags"
    return DagBag(dag_folder=str(dags_folder), include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, f"DAG import errors found: {dagbag.import_errors}"


def test_dag_loaded(dagbag: DagBag) -> None:
    dag = dagbag.get_dag("debi_ingestor_glue_runner")
    assert dag is not None


def test_tasks_and_dependencies(dagbag: DagBag) -> None:
    dag = dagbag.get_dag("debi_ingestor_glue_runner")
    assert dag is not None

    build_event = dag.get_task("build_event")
    run_glue = dag.get_task("run_glue_job")

    assert isinstance(build_event, PythonOperator)
    assert isinstance(run_glue, AwsGlueJobOperator)

    # build_event >> run_glue_job
    assert run_glue.task_id in build_event.downstream_task_ids
    assert build_event.task_id in run_glue.upstream_task_ids


def test_glue_operator_args_shape(dagbag: DagBag) -> None:
    dag = dagbag.get_dag("debi_ingestor_glue_runner")
    assert dag is not None

    run_glue = dag.get_task("run_glue_job")
    assert isinstance(run_glue, AwsGlueJobOperator)

    # Basic operator config sanity checks
    assert run_glue.aws_conn_id == "aws_default"
    assert run_glue.job_name == "unit-test-glue-job"

    # Ensure your runner flags exist (so argparse won't fail in Glue)
    script_args = run_glue.script_args
    assert isinstance(script_args, dict)

    expected_keys = {
        "--env",
        "--run_mode",
        "--table",
        "--vendor",
        "--repo_name",
        "--file_path",
        "--github_token",
        "--start_date",
        "--end_date",
        "--extra_env",
        "--event",
    }
    missing = expected_keys - set(script_args.keys())
    assert not missing, f"Missing script_args keys: {missing}"

    # Spot-check templated fields are strings (not required, but nice)
    assert isinstance(script_args["--env"], str)
    assert isinstance(script_args["--event"], str)
