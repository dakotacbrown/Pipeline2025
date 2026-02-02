import importlib.util
import sys
import types
import uuid
from datetime import datetime

import pytest

DAG_FILE_RELATIVE_PATH = "dags/generic/aws_cost_triggerer.py"

STUB_MODULES = (
    "dags.common.dag_utilities",
    "dags.common.slack",
    "dags.common.user_defined_filters",
)


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
def isolated_stub_modules(monkeypatch):
    """
    Install stubs into sys.modules, but restore sys.modules exactly as it was after the test.
    This prevents breaking other tests that import real dags.common.* modules.
    """
    original = {name: sys.modules.get(name) for name in STUB_MODULES}

    # ---- dags.common.dag_utilities stub ----
    dag_utils_name = "dags.common.dag_utilities"
    dag_utils = types.ModuleType(dag_utils_name)

    def extract_value(*args, **kwargs):
        return args[0] if args else None

    def failover_managed_dag_tag():
        return "failover-managed"

    def print_next_date(cursor_str: str):
        return "2023-09-02"

    def set_progress_operator(*, task_id: str, key: str, value: str):
        from airflow.operators.empty import EmptyOperator

        op = EmptyOperator(task_id=task_id)
        op.progress_key = key
        op.progress_value = value
        return op

    dag_utils.extract_value = extract_value
    dag_utils.failover_managed_dag_tag = failover_managed_dag_tag
    dag_utils.print_next_date = print_next_date
    dag_utils.set_progress_operator = set_progress_operator

    # ---- dags.common.slack stub ----
    slack_name = "dags.common.slack"
    slack_mod = types.ModuleType(slack_name)

    def task_fail_slack_alert(*args, **kwargs):
        return None

    slack_mod.task_fail_slack_alert = task_fail_slack_alert

    # ---- dags.common.user_defined_filters stub ----
    udf_name = "dags.common.user_defined_filters"
    udf_mod = types.ModuleType(udf_name)

    def ts_nodash_to_YYYYMMDDHHmmss(value):
        return value

    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = ts_nodash_to_YYYYMMDDHHmmss

    # Install/override in sys.modules for THIS test only
    monkeypatch.setitem(sys.modules, dag_utils_name, dag_utils)
    monkeypatch.setitem(sys.modules, slack_name, slack_mod)
    monkeypatch.setitem(sys.modules, udf_name, udf_mod)

    yield

    # Restore sys.modules to its exact prior state
    for name in STUB_MODULES:
        prior = original[name]
        if prior is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = prior


@pytest.fixture()
def loaded_dag_module(monkeypatch, isolated_stub_modules):
    repo_root = _project_root()
    dag_file = repo_root / DAG_FILE_RELATIVE_PATH
    if not dag_file.exists():
        raise FileNotFoundError(f"Expected DAG file at: {dag_file}")

    # safer than manual sys.path edits; auto-reverts
    monkeypatch.syspath_prepend(str(repo_root))

    # Patch Variable.get BEFORE import (module builds DAG at import time)
    import airflow.models

    def fake_variable_get(key, default_var=None, default=None):
        fallback = default if default is not None else default_var
        if key == "AWS_COST_CURSOR":
            return "2023-09-01"
        return fallback

    monkeypatch.setattr(
        airflow.models.Variable, "get", staticmethod(fake_variable_get)
    )

    # Unique module name so it won't collide with other tests importing this DAG
    module_name = f"aws_cost_triggerer_under_test_{uuid.uuid4().hex}"
    module = _load_module_from_path(module_name, dag_file)

    yield module

    # Cleanup: remove the imported dag module from sys.modules
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
