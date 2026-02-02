import importlib.util
import sys
import uuid
from datetime import datetime

import pytest

DAG_FILE_RELATIVE_PATH = "dags/generic/aws_cost_triggerer.py"


def _project_root():
    return __import__("pathlib").Path(__file__).resolve().parents[2]


def _load_module_from_path(module_name: str, file_path):
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module spec from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def airflow_test_env(monkeypatch, tmp_path):
    monkeypatch.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))


@pytest.fixture()
def loaded_dag_module(monkeypatch):
    repo_root = _project_root()
    dag_file = repo_root / DAG_FILE_RELATIVE_PATH
    if not dag_file.exists():
        raise FileNotFoundError(f"Expected DAG file at: {dag_file}")

    # Ensure `dags.*` imports resolve, and auto-revert after test
    monkeypatch.syspath_prepend(str(repo_root))

    # ---- Patch Airflow Variable.get BEFORE importing DAG (DAG builds at import time) ----
    import airflow.models

    def fake_variable_get(key, default_var=None, default=None):
        fallback = default if default is not None else default_var
        if key == "AWS_COST_CURSOR":
            return "2023-09-01"
        return fallback

    monkeypatch.setattr(
        airflow.models.Variable, "get", staticmethod(fake_variable_get)
    )

    # ---- Patch real project modules (do NOT replace sys.modules) ----
    import dags.common.dag_utilities as dag_utils
    import dags.common.slack as slack_mod
    import dags.common.user_defined_filters as udf_mod

    monkeypatch.setattr(
        dag_utils, "failover_managed_dag_tag", lambda: "failover-managed"
    )
    monkeypatch.setattr(
        dag_utils, "print_next_date", lambda cursor: "2023-09-02"
    )
    monkeypatch.setattr(
        dag_utils, "extract_value", lambda *a, **k: a[0] if a else None
    )

    # set_progress_operator must return an operator; use EmptyOperator for test
    from airflow.operators.empty import EmptyOperator

    def fake_set_progress_operator(*, task_id: str, key: str, value: str):
        op = EmptyOperator(task_id=task_id)
        op.progress_key = key
        op.progress_value = value
        return op

    monkeypatch.setattr(
        dag_utils, "set_progress_operator", fake_set_progress_operator
    )

    monkeypatch.setattr(
        slack_mod, "task_fail_slack_alert", lambda *a, **k: None
    )
    monkeypatch.setattr(udf_mod, "ts_nodash_to_YYYYMMDDHHmmss", lambda v: v)

    # ---- Import DAG module under a unique name (avoid caching collisions) ----
    module_name = f"aws_cost_triggerer_under_test_{uuid.uuid4().hex}"
    module = _load_module_from_path(module_name, dag_file)

    yield module

    sys.modules.pop(module_name, None)


def test_dag_metadata(loaded_dag_module):
    dag = loaded_dag_module.dag

    assert dag.dag_id == "debi_aws_cost_glue_triggerer"
    assert dag.schedule_interval == "0 5 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    assert "invoke-glue" in dag.tags
    assert "airflow-2.x.x-compatible" in dag.tags
    assert "failover-managed" in dag.tags


def test_tasks_exist_and_types(loaded_dag_module):
    dag = loaded_dag_module.dag

    assert set(dag.task_ids) == {
        "select_query_date",
        "trigger_aws_cost_generic_dag",
        "resolve_cursor",
        "update_progress",
    }

    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    assert isinstance(dag.get_task("select_query_date"), PythonOperator)
    assert isinstance(
        dag.get_task("trigger_aws_cost_generic_dag"), TriggerDagRunOperator
    )
    assert isinstance(dag.get_task("resolve_cursor"), PythonOperator)
    assert isinstance(dag.get_task("update_progress"), EmptyOperator)


def test_trigger_operator_configuration(loaded_dag_module):
    dag = loaded_dag_module.dag
    t = dag.get_task("trigger_aws_cost_generic_dag")

    assert t.trigger_dag_id == "debi_generic_ingester_glue_runner"
    assert t.wait_for_completion is True
    assert t.conf["vendor"] == "aws_cost"
    assert (
        t.conf["start_date"] == "{{ task_instance.xcom_pull(key='from_date') }}"
    )
    assert t.conf["end_date"] == "{{ task_instance.xcom_pull(key='to_date') }}"


def test_task_dependencies(loaded_dag_module):
    dag = loaded_dag_module.dag
    assert dag.get_task("select_query_date").downstream_task_ids == {
        "trigger_aws_cost_generic_dag"
    }
    assert dag.get_task("trigger_aws_cost_generic_dag").downstream_task_ids == {
        "resolve_cursor"
    }
    assert dag.get_task("resolve_cursor").downstream_task_ids == {
        "update_progress"
    }


def test_select_query_date_fn_prefers_params_value(loaded_dag_module):
    fn = loaded_dag_module.select_query_date_fn
    out = fn(
        params={"query_date": "2024-10-27"},
        now_fn=lambda: datetime(2024, 10, 30),
    )
    assert out == "2024-10-27"


def test_select_query_date_fn_falls_back_to_cursor(
    loaded_dag_module, monkeypatch
):
    monkeypatch.setattr(
        loaded_dag_module, "aws_cost_cursor", lambda: "2023-09-01"
    )
    fn = loaded_dag_module.select_query_date_fn
    out = fn(params={}, now_fn=lambda: datetime(2024, 10, 30))
    assert out == "2023-09-01"


def test_resolve_cursor_fn_leaves_cursor_unchanged_when_param_provided(
    loaded_dag_module, monkeypatch
):
    monkeypatch.setattr(
        loaded_dag_module, "aws_cost_cursor", lambda: "2023-09-01"
    )
    fn = loaded_dag_module.resolve_cursor_fn
    out = fn(params={"query_date": "2024-10-27"})
    assert out == "2023-09-01"


def test_resolve_cursor_fn_advances_cursor_when_no_param(
    loaded_dag_module, monkeypatch
):
    monkeypatch.setattr(
        loaded_dag_module, "aws_cost_cursor", lambda: "2023-09-01"
    )
    monkeypatch.setattr(
        loaded_dag_module, "print_next_date", lambda cursor: "2023-09-02"
    )
    fn = loaded_dag_module.resolve_cursor_fn
    out = fn(params={})
    assert out == "2023-09-02"
