# tests/unit/test_debi_ingestor_glue_runner_dag.py
from __future__ import annotations

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# Import your dag module (update this path to your actual file)
from dags.debi_ingestor_glue_runner import dag  # or DAG_ID if you export it


def test_dag_has_expected_id() -> None:
    assert dag.dag_id == "debi_ingestor_glue_runner"


def test_tasks() -> None:
    assert {t.task_id for t in dag.tasks} == {"run_debi_ingestor_glue"}

    t = dag.get_task("run_debi_ingestor_glue")
    assert isinstance(t, GlueJobOperator)


def test_script_args_shape() -> None:
    t = dag.get_task("run_debi_ingestor_glue")
    expected_keys = {
        "--env",
        "--table",
        "--run_mode",
        "--event",
        "--file_path",
        "--repo_name",
        "--github_token",
        "--start_date",
        "--end_date",
    }
    assert expected_keys.issubset(set(t.script_args.keys()))
