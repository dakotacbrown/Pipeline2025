import importlib.util
import sys
import types
from datetime import datetime

import pytest

DAG_FILE_RELATIVE_PATH = "dags/generic/aws_cost_triggerer.py"
DAG_MODULE_NAME = "aws_cost_triggerer_under_test"


def _project_root():
    # <repo>/tests/dags/test_aws_cost_triggerer.py -> parents[2] == <repo>
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
    # keeps Airflow quieter + avoids writing into a real AIRFLOW_HOME
    monkeypatch.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))


@pytest.fixture()
def loaded_dag_module(monkeypatch):
    repo_root = _project_root()
    dag_file = repo_root / DAG_FILE_RELATIVE_PATH
    if not dag_file.exists():
        raise FileNotFoundError(f"Expected DAG file at: {dag_file}")

    # allow `from dags....` imports to resolve
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    # ------------------------------------------------------------------
    # 1) Patch Variable.get BEFORE importing the DAG module
    # ------------------------------------------------------------------
    import airflow.models

    def fake_variable_get(key, default_var=None, default=None):
        # support either signature used by Variable.get in different calls
        fallback = default if default is not None else default_var
        if key == "AWS_COST_CURSOR":
            return "2023-09-01"
        return fallback

    monkeypatch.setattr(
        airflow.models.Variable,
        "get",
        staticmethod(fake_variable_get),
    )

    # ------------------------------------------------------------------
    # 2) Stub house modules imported by the DAG BEFORE module import
    # ------------------------------------------------------------------

    # dags.common.dag_utilities
    dag_utils_name = "dags.common.dag_utilities"
    dag_utils = types.ModuleType(dag_utils_name)

    def extract_value(*args, **kwargs):
        # only used as a user_defined_macro; keep harmless
        return args[0] if args else None

    def failover_managed_dag_tag():
        return "failover-managed"

    def print_next_date(cursor_str: str):
        # deterministic "next date"
        # (real impl probably adds 1 day; exact logic isn't needed for DAG import tests)
        return "2023-09-02"

    def set_progress_operator(*, task_id: str, key: str, value: str):
        # return a real Airflow operator so the DAG can register the task
        from airflow.operators.empty import EmptyOperator

        op = EmptyOperator(task_id=task_id)
        # attach for assertions
        op.progress_key = key
        op.progress_value = value
        return op

    dag_utils.extract_value = extract_value
    dag_utils.failover_managed_dag_tag = failover_managed_dag_tag
    dag_utils.print_next_date = print_next_date
    dag_utils.set_progress_operator = set_progress_operator
    sys.modules[dag_utils_name] = dag_utils

    # dags.common.slack
    slack_name = "dags.common.slack"
    slack_mod = types.ModuleType(slack_name)

    def task_fail_slack_alert(*args, **kwargs):
        return None

    slack_mod.task_fail_slack_alert = task_fail_slack_alert
    sys.modules[slack_name] = slack_mod

    # dags.common.user_defined_filters
    udf_name = "dags.common.user_defined_filters"
    udf_mod = types.ModuleType(udf_name)

    def ts_nodash_to_YYYYMMDDHHmmss(value):
        return value

    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = ts_nodash_to_YYYYMMDDHHmmss
    sys.modules[udf_name] = udf_mod

    # ------------------------------------------------------------------
    # 3) Import DAG module (this will build `dag = aws_cost()` immediately)
    # ------------------------------------------------------------------
    return _load_module_from_path(DAG_MODULE_NAME, dag_file)


def test_dag_metadata(loaded_dag_module):
    dag = loaded_dag_module.dag

    assert dag.dag_id == "debi_aws_cost_glue_triggerer"
    assert dag.schedule_interval == "0 5 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    assert "invoke-glue" in dag.tags
    assert "airflow-2.x.x-compatible" in dag.tags
    assert "failover-managed" in dag.tags

    # start_date should be tz-aware (pendulum UTC)
    assert dag.start_date is not None
    assert str(dag.start_date.tzinfo) in ("UTC", "Timezone('UTC')", "UTC+00:00")


def test_dag_has_query_date_param(loaded_dag_module):
    dag = loaded_dag_module.dag

    assert "query_date" in dag.params
    # Param is an object; keep assertions tolerant across Airflow patch versions
    param = dag.params["query_date"]
    assert getattr(param, "default", None) is None


def test_tasks_exist_and_types(loaded_dag_module):
    dag = loaded_dag_module.dag

    expected_task_ids = {
        "select_query_date",
        "trigger_aws_cost_generic_dag",
        "resolve_cursor",
        "update_progress",
    }
    assert set(dag.task_ids) == expected_task_ids

    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    select_task = dag.get_task("select_query_date")
    trigger_task = dag.get_task("trigger_aws_cost_generic_dag")
    resolve_task = dag.get_task("resolve_cursor")
    update_task = dag.get_task("update_progress")

    assert isinstance(select_task, PythonOperator)
    assert isinstance(trigger_task, TriggerDagRunOperator)
    assert isinstance(resolve_task, PythonOperator)
    assert isinstance(update_task, EmptyOperator)

    assert select_task.python_callable == loaded_dag_module.select_query_date_fn
    assert resolve_task.python_callable == loaded_dag_module.resolve_cursor_fn


def test_trigger_operator_configuration(loaded_dag_module):
    dag = loaded_dag_module.dag
    trigger_task = dag.get_task("trigger_aws_cost_generic_dag")

    assert trigger_task.trigger_dag_id == "debi_generic_ingester_glue_runner"
    assert trigger_task.wait_for_completion is True

    assert trigger_task.conf["vendor"] == "aws_cost"
    assert (
        trigger_task.conf["start_date"]
        == "{{ task_instance.xcom_pull(key='from_date') }}"
    )
    assert (
        trigger_task.conf["end_date"]
        == "{{ task_instance.xcom_pull(key='to_date') }}"
    )


def test_task_dependencies(loaded_dag_module):
    dag = loaded_dag_module.dag

    select_task = dag.get_task("select_query_date")
    trigger_task = dag.get_task("trigger_aws_cost_generic_dag")
    resolve_task = dag.get_task("resolve_cursor")
    update_task = dag.get_task("update_progress")

    assert select_task.downstream_task_ids == {"trigger_aws_cost_generic_dag"}
    assert trigger_task.upstream_task_ids == {"select_query_date"}

    assert trigger_task.downstream_task_ids == {"resolve_cursor"}
    assert resolve_task.upstream_task_ids == {"trigger_aws_cost_generic_dag"}

    assert resolve_task.downstream_task_ids == {"update_progress"}
    assert update_task.upstream_task_ids == {"resolve_cursor"}


def test_select_query_date_fn_prefers_params_value(loaded_dag_module):
    fn = loaded_dag_module.select_query_date_fn
    params = {"query_date": "2024-10-27"}

    # provide deterministic now_fn; should still return params date
    out = fn(params=params, now_fn=lambda: datetime(2024, 10, 30))
    assert out == "2024-10-27"


def test_select_query_date_fn_falls_back_to_cursor(
    loaded_dag_module, monkeypatch
):
    # Make fallback deterministic
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
