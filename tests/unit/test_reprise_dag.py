import importlib.util
import sys
import types
from pathlib import Path

import pytest

DAG_FILE_RELATIVE_PATH = Path("dags/generic/reprise_triggerer.py")
DAG_MODULE_NAME = "reprise_triggerer_under_test"


def _project_root() -> Path:
    """
    Assumes tests live in: <repo>/tests/dags/test_reprise_triggerer.py
    """
    return Path(__file__).resolve().parents[2]


def _load_module_from_path(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module spec from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def stub_common_modules(monkeypatch):
    """
    The DAG imports:
      - dags.common.dag_utilities: extract_value, failover_managed_dag_tag, print_next_date, set_progress_operator
      - dags.common.slack: task_fail_slack_alert
      - dags.common.user_defined_filters: ts_nodash_to_YYYYMMDDHHmmss

    We stub these so the DAG can be imported in unit tests without pulling your full project.
    """
    # Ensure packages exist
    monkeypatch.setitem(sys.modules, "dags", types.ModuleType("dags"))
    monkeypatch.setitem(
        sys.modules, "dags.common", types.ModuleType("dags.common")
    )

    dag_utils = types.ModuleType("dags.common.dag_utilities")

    def extract_value(*args, **kwargs):
        return None

    def failover_managed_dag_tag():
        return "failover-managed"

    def print_next_date(date_str: str) -> str:
        # minimal stub; tests will monkeypatch this where needed
        return f"{date_str}-next"

    # Build an Airflow operator that stores key/value so we can assert on it
    try:
        from airflow.models import BaseOperator
    except Exception:  # pragma: no cover
        BaseOperator = object  # fallback if Airflow isn't present

    class ProgressOperator(BaseOperator):
        template_fields = ("value",)

        def __init__(self, *, task_id: str, key: str, value: str, **kwargs):
            super().__init__(task_id=task_id, **kwargs)
            self.key = key
            self.value = value

    def set_progress_operator(*, task_id: str, key: str, value: str, **kwargs):
        # In your DAG it’s used like:
        #   set_progress_operator(task_id="update_progress", key=reprise_cursor_key, value="...jinja...")
        return ProgressOperator(task_id=task_id, key=key, value=value, **kwargs)

    dag_utils.extract_value = extract_value
    dag_utils.failover_managed_dag_tag = failover_managed_dag_tag
    dag_utils.print_next_date = print_next_date
    dag_utils.set_progress_operator = set_progress_operator

    slack_mod = types.ModuleType("dags.common.slack")

    def task_fail_slack_alert(*args, **kwargs):
        return None

    slack_mod.task_fail_slack_alert = task_fail_slack_alert

    filters_mod = types.ModuleType("dags.common.user_defined_filters")

    def ts_nodash_to_YYYYMMDDHHmmss(value):
        return value

    filters_mod.ts_nodash_to_YYYYMMDDHHmmss = ts_nodash_to_YYYYMMDDHHmmss

    monkeypatch.setitem(sys.modules, "dags.common.dag_utilities", dag_utils)
    monkeypatch.setitem(sys.modules, "dags.common.slack", slack_mod)
    monkeypatch.setitem(
        sys.modules, "dags.common.user_defined_filters", filters_mod
    )

    return {
        "dag_utils": dag_utils,
        "slack": slack_mod,
        "filters": filters_mod,
    }


@pytest.fixture
def dag_module(stub_common_modules):
    dag_path = _project_root() / DAG_FILE_RELATIVE_PATH
    assert dag_path.exists(), f"Could not find DAG file at: {dag_path}"
    return _load_module_from_path(DAG_MODULE_NAME, dag_path)


@pytest.fixture
def dag(dag_module):
    assert hasattr(
        dag_module, "dag"
    ), "Expected module to define `dag = reprise()`"
    return dag_module.dag


def test_dag_metadata_and_tasks(dag):
    assert dag.dag_id == "debi_reprise_glue_triggerer"

    # Airflow 2.10.5: schedule can be surfaced as .schedule_interval or .schedule depending on config
    schedule = getattr(dag, "schedule_interval", None) or getattr(
        dag, "schedule", None
    )
    assert schedule == "0 5 * * *"

    assert dag.catchup is False
    assert dag.max_active_runs == 1

    # tags are a set-like collection
    assert "invoke-glue" in dag.tags
    assert "airflow-2.x.x-compatible" in dag.tags
    assert (
        "failover-managed" in dag.tags
    )  # from our stubbed failover_managed_dag_tag()

    expected_task_ids = {
        "select_query_date",
        "trigger_reprise_generic_dag",
        "resolve_cursor",
        "update_progress",
    }
    assert set(dag.task_ids) == expected_task_ids


def test_task_dependencies(dag):
    select_task = dag.get_task("select_query_date")
    trigger_task = dag.get_task("trigger_reprise_generic_dag")
    resolve_task = dag.get_task("resolve_cursor")
    progress_task = dag.get_task("update_progress")

    assert select_task.downstream_task_ids == {"trigger_reprise_generic_dag"}
    assert trigger_task.upstream_task_ids == {"select_query_date"}
    assert trigger_task.downstream_task_ids == {"resolve_cursor"}
    assert resolve_task.upstream_task_ids == {"trigger_reprise_generic_dag"}
    assert resolve_task.downstream_task_ids == {"update_progress"}
    assert progress_task.upstream_task_ids == {"resolve_cursor"}


def test_trigger_dag_run_operator_conf(dag):
    trigger_task = dag.get_task("trigger_reprise_generic_dag")

    # TriggerDagRunOperator has attribute trigger_dag_id in Airflow 2.x
    assert (
        getattr(trigger_task, "trigger_dag_id")
        == "debi_generic_ingester_glue_runner"
    )

    conf = getattr(trigger_task, "conf")
    assert conf["vendor"] == "reprise"
    assert (
        conf["start_date"] == "{{ task_instance.xcom_pull(key='from_date') }}"
    )
    assert conf["end_date"] == "{{ task_instance.xcom_pull(key='to_date') }}"


def test_python_operator_wiring(dag, dag_module):
    select_task = dag.get_task("select_query_date")
    resolve_task = dag.get_task("resolve_cursor")

    assert (
        getattr(select_task, "python_callable")
        == dag_module.select_query_date_fn
    )
    assert (
        getattr(resolve_task, "python_callable") == dag_module.resolve_cursor_fn
    )

    # The DAG sets trigger_rule="none_failed" for resolve_cursor
    assert getattr(resolve_task, "trigger_rule") == "none_failed"


def test_update_progress_operator_receives_key_and_value_template(dag):
    progress_task = dag.get_task("update_progress")

    # From code:
    #   key=reprise_cursor_key (which is "REPRISE_CURSOR")
    #   value="{{ task_instance.xcom_pull('{resolve_cursor_task_name}', key='return_value') }}"
    assert getattr(progress_task, "key") == "REPRISE_CURSOR"

    value = getattr(progress_task, "value")
    assert (
        value
        == "{{ task_instance.xcom_pull('resolve_cursor', key='return_value') }}"
    )


class _FakeTI:
    def __init__(self):
        self.pushed = []

    def xcom_push(self, *, key, value):
        self.pushed.append((key, value))


def test_select_query_date_fn_uses_provided_params(dag_module, monkeypatch):
    ti = _FakeTI()

    def _boom(*args, **kwargs):
        raise AssertionError(
            "Should not be called when from_date/to_date are provided"
        )

    monkeypatch.setattr(dag_module, "resolve_cursor_fn", _boom)
    monkeypatch.setattr(dag_module, "print_next_date", _boom)

    dag_module.select_query_date_fn(
        ti,
        params={"from_date": "2024-01-01", "to_date": "2024-01-02"},
    )

    assert ("from_date", "2024-01-01") in ti.pushed
    assert ("to_date", "2024-01-02") in ti.pushed


def test_select_query_date_fn_falls_back_to_cursor(dag_module, monkeypatch):
    ti = _FakeTI()

    monkeypatch.setattr(
        dag_module, "resolve_cursor_fn", lambda params: "2024-01-10"
    )
    monkeypatch.setattr(
        dag_module, "print_next_date", lambda date_str: "2024-01-11"
    )

    dag_module.select_query_date_fn(
        ti,
        params={"from_date": None, "to_date": None},
    )

    assert ti.pushed == [("from_date", "2024-01-10"), ("to_date", "2024-01-11")]


def test_resolve_cursor_fn_behavior(dag_module, monkeypatch):
    monkeypatch.setattr(dag_module, "reprise_cursor", lambda: "2020-09-01")
    monkeypatch.setattr(
        dag_module, "print_next_date", lambda date_str: "2020-09-02"
    )

    # If query_date is present, cursor is unchanged
    assert (
        dag_module.resolve_cursor_fn({"query_date": "anything"}) == "2020-09-01"
    )

    # Otherwise cursor advances
    assert dag_module.resolve_cursor_fn({}) == "2020-09-02"
