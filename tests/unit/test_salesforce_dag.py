import importlib
import sys
from unittest.mock import patch

import pytest
from airflow.models.baseoperator import BaseOperator


class DummyGlueJobOperator(BaseOperator):
    """
    Minimal stand-in so unit tests don't depend on provider internals.
    We only need params saved on the operator to assert script_args, etc.
    """

    def __init__(
        self,
        *,
        job_name,
        aws_conn_id,
        region_name,
        script_args,
        wait_for_completion=True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.script_args = script_args
        self.wait_for_completion = wait_for_completion


def _variable_get_side_effect(key, default=None):
    mapping = {
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "qa",
        "INGESTER_TABLES": '["events_api", "contacts_api"]',
        "INGESTER_VENDOR": "salesforce",
        "INGESTER_CONFIG_PATH": "s3://bucket/config.yaml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "CISCOREDATASERVICES_GITHUB_PASSWORD": "fake-token",
        "INGESTER_START_DATE": "2000-01-01",
        "INGESTER_END_DATE": "2025-01-01",
        "INGESTER_ENV_VARS": '{"qa": {"FOO": "bar"}}',
        "INGESTER_EXCHANGE_EXTRAS": '{"exchange_headers": {"x":"y"}}',
        "INGESTER_DATA_EXTRAS": "{}",  # empty dict path
    }
    return mapping.get(key, default)


@pytest.fixture()
def dag():
    module_name = "dags.salesforce.salesforce_ingester"

    # Make sure we import fresh each test run
    sys.modules.pop(module_name, None)

    with patch("dags.common.dag_utilities.get_shairflow_environment", return_value="qa"), patch(
        "dags.common.dag_utilities.get_shairflow_region", return_value="us-east-1"
    ), patch(
        "airflow.models.Variable.get", side_effect=_variable_get_side_effect
    ), patch(
        "airflow.providers.amazon.aws.operators.glue.GlueJobOperator",
        DummyGlueJobOperator,
    ):
        mod = importlib.import_module(module_name)
        importlib.reload(mod)
        return mod.dag


def test_dag_loaded(dag):
    assert dag is not None
    assert dag.dag_id == "debi_ingestor_glue_runner"


def test_tasks_present(dag):
    task_ids = set(dag.task_ids)
    assert "build_event" in task_ids
    assert "run_glue_job__events_api" in task_ids
    assert "run_glue_job__contacts_api" in task_ids


def test_dependencies(dag):
    build = dag.get_task("build_event")
    assert "run_glue_job__events_api" in build.downstream_task_ids
    assert "run_glue_job__contacts_api" in build.downstream_task_ids

    events = dag.get_task("run_glue_job__events_api")
    assert "build_event" in events.upstream_task_ids


def test_glue_operator_args_shape(dag):
    t = dag.get_task("run_glue_job__events_api")

    assert t.job_name == "etl-job"
    assert t.aws_conn_id == "etl-net-conn"
    assert t.region_name == "us-east-1"
    assert t.wait_for_completion is True

    args = t.script_args
    assert args["--env"] == "qa"
    assert args["--run_mode"] == "qa"
    assert args["--table"] == "events_api"
    assert args["--vendor"] == "salesforce"
    assert args["--repo_name"] == "config_management"
    assert args["--file_path"] == "s3://bucket/config.yaml"
    assert args["--github_token"] == "fake-token"
    assert args["--start_date"] == "2000-01-01"
    assert args["--end_date"] == "2025-01-01"
    assert args["--event"] == "{{ ti.xcom_pull(task_ids='build_event') }}"
