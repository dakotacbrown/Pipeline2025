import importlib
import importlib.util
import sys
import types
from pathlib import Path

import pytest

DAG_FILE_RELATIVE_PATH = Path("dags/generic/salesforce_triggerer.py")
DAG_MODULE_NAME = "salesforce_triggerer_under_test"


def _project_root() -> Path:
    """
    Assumes tests live in: <repo>/tests/.../test_salesforce_ingester_triggerer.py
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


@pytest.fixture(autouse=True)
def airflow_test_env(monkeypatch, tmp_path):
    """
    Make Airflow imports calmer in unit tests.
    """
    monkeypatch.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))


@pytest.fixture()
def loaded_dag_module(monkeypatch):
    """
    Patch Variable.get and stub failover_managed_dag_tag (if needed)
    BEFORE importing the DAG module (since dag_scheduler() executes at import time).
    """
    repo_root = _project_root()
    dag_file = repo_root / DAG_FILE_RELATIVE_PATH
    if not dag_file.exists():
        raise FileNotFoundError(f"Expected DAG file at: {dag_file}")

    # Ensure repo root is on sys.path so `from dags.common...` imports can work
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    # ---- Patch airflow.models.Variable.get BEFORE module import ----
    import airflow.models

    values = {
        "C1S_SALESFORCE_USERNAME": "test-user",
        "C1S_SALESFORCE_PASSWORD": "test-pass",
        "C1S_SALESFORCE_CLIENTID": "test-client-id",
        "C1S_SALESFORCE_CLIENTSECRET": "test-client-secret",
    }

    # Airflow calls Variable.get(key, default_var=..., deserialize_json=...)
    def fake_variable_get(
        key, default_var=None, deserialize_json=False, **kwargs
    ):
        return values.get(key, default_var)

    monkeypatch.setattr(
        airflow.models.Variable,
        "get",
        staticmethod(fake_variable_get),
    )

    # ---- Stub dags.common.dag_utilities.failover_managed_dag_tag if import fails ----
    util_mod_name = "dags.common.dag_utilities"
    try:
        importlib.import_module(util_mod_name)
    except Exception:
        stub = types.ModuleType(util_mod_name)

        def failover_managed_dag_tag():
            return "failover-managed"

        stub.failover_managed_dag_tag = failover_managed_dag_tag
        monkeypatch.setitem(sys.modules, util_mod_name, stub)

    # Load the DAG module (it will build the DAG immediately)
    module = _load_module_from_path(DAG_MODULE_NAME, dag_file)
    return module


def _get_schedule_str(dag) -> str:
    """
    Airflow 2.x can expose schedule via schedule_interval or schedule.
    """
    if hasattr(dag, "schedule") and dag.schedule is not None:
        return str(dag.schedule)
    if hasattr(dag, "schedule_interval"):
        return str(dag.schedule_interval)
    return ""


def test_dag_metadata(loaded_dag_module):
    dag = loaded_dag_module.dag_scheduler

    assert dag.dag_id == "debi_salesforce_glue_triggerer"
    assert _get_schedule_str(dag) == "0 11,16,19 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    assert dag.start_date is not None
    assert str(dag.start_date.tzinfo) in ("UTC", "Timezone('UTC')", "UTC+00:00")

    assert "airflow-2.x.x-compatible" in dag.tags
    assert "failover-managed" in dag.tags


def test_tasks_exist_and_counts_match(loaded_dag_module):
    dag = loaded_dag_module.dag_scheduler

    assert set(dag.task_ids) == {
        "start",
        "trigger_salesforce_generic_dag",
        "trigger_revcloud_generic_dag",
        "join",
    }
    assert len(dag.tasks) == 4


def test_trigger_tasks_are_configured(loaded_dag_module):
    dag = loaded_dag_module.dag_scheduler

    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    t_salesforce = dag.get_task("trigger_salesforce_generic_dag")
    t_revcloud = dag.get_task("trigger_revcloud_generic_dag")

    assert isinstance(t_salesforce, TriggerDagRunOperator)
    assert isinstance(t_revcloud, TriggerDagRunOperator)

    assert t_salesforce.trigger_dag_id == "debi_generic_ingester_glue_runner"
    assert t_revcloud.trigger_dag_id == "debi_generic_ingester_glue_runner"

    assert t_salesforce.wait_for_completion is True
    assert t_revcloud.wait_for_completion is True

    expected_creds = {
        "username": "test-user",
        "password": "test-pass",
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
    }

    # Verify full conf payloads including vendor
    assert t_salesforce.conf == {
        "vendor": "salesforce",
        "credentials": expected_creds,
    }
    assert t_revcloud.conf == {
        "vendor": "revcloud",
        "credentials": expected_creds,
    }


def test_parallel_structure_start_to_triggers_and_join(loaded_dag_module):
    dag = loaded_dag_module.dag_scheduler

    start = dag.get_task("start")
    t_salesforce = dag.get_task("trigger_salesforce_generic_dag")
    t_revcloud = dag.get_task("trigger_revcloud_generic_dag")
    join = dag.get_task("join")

    # Both triggers depend on start (parallel fan-out)
    assert t_salesforce.upstream_task_ids == {"start"}
    assert t_revcloud.upstream_task_ids == {"start"}

    # No dependency between triggers
    assert "trigger_revcloud_generic_dag" not in t_salesforce.upstream_task_ids
    assert (
        "trigger_revcloud_generic_dag" not in t_salesforce.downstream_task_ids
    )
    assert "trigger_salesforce_generic_dag" not in t_revcloud.upstream_task_ids
    assert (
        "trigger_salesforce_generic_dag" not in t_revcloud.downstream_task_ids
    )

    # Join depends on both triggers (fan-in)
    assert join.upstream_task_ids == {
        "trigger_salesforce_generic_dag",
        "trigger_revcloud_generic_dag",
    }

    # Optional extra checks (nice to have)
    assert start.downstream_task_ids == {
        "trigger_salesforce_generic_dag",
        "trigger_revcloud_generic_dag",
    }
    assert t_salesforce.downstream_task_ids == {"join"}
    assert t_revcloud.downstream_task_ids == {"join"}
