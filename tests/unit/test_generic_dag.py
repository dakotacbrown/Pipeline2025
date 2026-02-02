import importlib
import io
import sys
import types
from unittest.mock import MagicMock

import pytest

DAG_MODULE = "dags.generic.generic_ingester"


def _ensure_pkg(name: str):
    """Ensure a package-like module exists in sys.modules."""
    if name not in sys.modules:
        m = types.ModuleType(name)
        m.__path__ = []  # mark as package
        sys.modules[name] = m
    return sys.modules[name]


def _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template):
    """
    Import/reload the DAG module with patched Airflow Variables, get_current_context,
    and any project-local modules the DAG imports at parse-time.
    """
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", "/tmp")

    # ------------------------------------------------------------------
    # Provide fake project modules so "from dags.common.... import ..." works
    # ------------------------------------------------------------------
    _ensure_pkg("dags")
    _ensure_pkg("dags.common")
    _ensure_pkg("dags.generic")

    dag_utils_mod = types.ModuleType("dags.common.dag_utilities")
    dag_utils_mod.failover_managed_dag_tag = lambda: "failover"
    dag_utils_mod.get_bucket_name = lambda env, trunc_region: "my-bucket"
    dag_utils_mod.get_cls_oauth_endpoint = lambda env: "https://oauth.example"
    dag_utils_mod.get_shairflow_environment = lambda: "qa"
    dag_utils_mod.get_shairflow_region = lambda: "us-east-1"
    dag_utils_mod.get_truncated_shairflow_region = lambda: "us-east-1"
    sys.modules["dags.common.dag_utilities"] = dag_utils_mod

    slack_mod = types.ModuleType("dags.common.slack")
    slack_mod.task_fail_slack_alert = lambda *args, **kwargs: None
    sys.modules["dags.common.slack"] = slack_mod

    udf_mod = types.ModuleType("dags.common.user_defined_filters")
    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = lambda s: s
    sys.modules["dags.common.user_defined_filters"] = udf_mod

    # ------------------------------------------------------------------
    # Patch Airflow get_current_context() since this DAG calls it at parse-time
    # ------------------------------------------------------------------
    import airflow.operators.python as airflow_py

    def fake_get_current_context():
        # The DAG reads context["dag_run"].conf
        dag_run = types.SimpleNamespace(
            conf={
                "vendor": "salesforce",
                "credentials": {},  # used only if INGESTER_DATA_EXTRAS is set
            }
        )
        return {"dag_run": dag_run}

    monkeypatch.setattr(
        airflow_py,
        "get_current_context",
        fake_get_current_context,
        raising=True,
    )

    # ------------------------------------------------------------------
    # Patch Variable.get for workflow + secrets
    # ------------------------------------------------------------------
    from airflow.models import Variable

    def fake_variable_get(key, default_var=None, deserialize_json=False):
        # New code uses CISCOREDATASERVICES_* keys
        if key == "CISCOREDATASERVICES_GITHUB_PASSWORD":
            return "gh-token"
        if key == "CISCOREDATASERVICES_EXCHANGE_ID":
            return "ex-id"
        if key == "CISCOREDATASERVICES_EXCHANGE_SECRET":
            return "ex-secret"

        # Workflow var selected by vendor from get_current_context(): SALESFORCE
        if key == "INGESTER_WORKFLOW_SALESFORCE":
            return workflow_var_dict if deserialize_json else workflow_var_dict

        return default_var

    monkeypatch.setattr(Variable, "get", staticmethod(fake_variable_get))

    # ------------------------------------------------------------------
    # Patch open() for get_copy_sql() task (it reads copy/copy_{table}.sql)
    # ------------------------------------------------------------------
    def fake_open(*args, **kwargs):
        return io.StringIO(copy_sql_template)

    monkeypatch.setattr("builtins.open", fake_open, raising=True)

    # Optional: patch boto3.client defensively (tasks shouldn't execute at parse time)
    import boto3

    monkeypatch.setattr(boto3, "client", MagicMock(), raising=True)

    # Import/reload the DAG module
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
        # NEW: dict of table -> dataset_id
        "INGESTER_TABLES": {"opportunity": "opportunity"},
        "INGESTER_RUN_MODE": "once",
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_SQL_PARAMS": {"DATABASE": "VALIDATION", "SCHEMA": "PUBLIC"},
        # Optional knobs in your new DAG
        "INGESTER_TESTING": False,
        "INGESTER_PYTHON_MODULES": None,
        "INGESTER_EXCHANGE": None,
    }


@pytest.fixture
def copy_sql_template():
    # NOTE: new code replaces "{{ params.s3_url }}" and "{{ params.target_table }}"
    return (
        "DELETE FROM {{ params.target_table }};\n"
        "COPY INTO {{ params.target_table }}\n"
        "FROM @VALIDATION_STAGE/{{ params.s3_url }}\n"
        "FILE_FORMAT = (TYPE = PARQUET)\n"
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;\n"
        "FORCE = TRUE;\n"
    )


def test_dag_builds_expected_tasks(
    monkeypatch, workflow_var_dict, copy_sql_template
):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    assert dag.dag_id == "debi_generic_ingester_glue_runner"

    # Shared zip task
    assert "latest_framework_zip" in dag.task_ids

    # Table-specific tasks (Opportunity -> safe id "opportunity")
    assert "build_event_opportunity" in dag.task_ids
    assert "run_glue_job_opportunity" in dag.task_ids
    assert "latest_jsonl_opportunity" in dag.task_ids
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

    # New code formats this via an f-string; expected final value is still the Jinja call
    assert (
        script_args["--event"]
        == "{{ ti.xcom_pull(task_ids='build_event_opportunity') }}"
    )


def test_load_table_sql_comes_from_copy_sql_xcom(
    monkeypatch, workflow_var_dict, copy_sql_template
):
    """
    load_table.sql should be an XComArg pointing at copy_sql_<table>.
    """
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    load = dag.get_task("load_table_opportunity")

    from airflow.models.xcom_arg import XComArg

    assert isinstance(load.sql, XComArg)
    assert load.sql.operator.task_id == "copy_sql_opportunity"

    # New code sets Snowflake conn_id explicitly
    assert load.conn_id == "snowflake_salesforce"


def test_dependencies(monkeypatch, workflow_var_dict, copy_sql_template):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    latest_zip = dag.get_task("latest_framework_zip")
    build_event = dag.get_task("build_event_opportunity")
    run_glue = dag.get_task("run_glue_job_opportunity")
    latest_jsonl = dag.get_task("latest_jsonl_opportunity")
    copy_sql = dag.get_task("copy_sql_opportunity")
    load = dag.get_task("load_table_opportunity")

    # latest_zip -> build_event -> run_glue
    assert build_event.task_id in latest_zip.downstream_task_ids
    assert run_glue.task_id in build_event.downstream_task_ids

    # run_glue -> latest_jsonl -> copy_sql -> load_table
    assert latest_jsonl.task_id in run_glue.downstream_task_ids
    assert copy_sql.task_id in latest_jsonl.downstream_task_ids
    assert load.task_id in copy_sql.downstream_task_ids
