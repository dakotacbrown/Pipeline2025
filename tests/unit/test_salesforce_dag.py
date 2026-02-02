import importlib.util
import sys
import types
from pathlib import Path

import pytest

DAG_FILE_RELATIVE_PATH = Path("dags/generic/salesforce_triggerer.py")
DAG_MODULE_NAME = "salesforce_triggerer_under_test"


def _project_root() -> Path:
    """
    Assumes tests live in: <repo>/tests/dags/test_salesforce_triggerer.py
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
    Patch Variable.get and (optionally) stub failover_managed_dag_tag
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
        "CIS_SALESFORCE_USERNAME": "test-user",
        "CIS_SALESFORCE_PASSWORD": "test-pass",
        "CIS_SALESFORCE_CLIENTID": "test-client-id",
        "CIS_SALESFORCE_CLIENTSECRET": "test-client-secret",
    }

    def fake_variable_get(key, default=None):
        return values.get(key, default)

    monkeypatch.setattr(
        airflow.models.Variable, "get", staticmethod(fake_variable_get)
    )

    # ---- Optionally stub dags.common.dag_utilities.failover_managed_dag_tag ----
    # If you prefer to use the real function, delete this block.
    util_mod_name = "dags.common.dag_utilities"
    if util_mod_name not in sys.modules:
        stub = types.ModuleType(util_mod_name)

        def failover_managed_dag_tag():
            return "failover-managed"

        stub.failover_managed_dag_tag = failover_managed_dag_tag
        sys.modules[util_mod_name] = stub
    else:
        # If it exists, you can still patch it to be deterministic:
        monkeypatch.setattr(
            sys.modules[util_mod_name],
            "failover_managed_dag_tag",
            lambda: "failover-managed",
        )

    # Now load the DAG module (it will build the DAG immediately)
    module = _load_module_from_path(DAG_MODULE_NAME, dag_file)
    return module


def test_dag_metadata(loaded_dag_module):
    dag = loaded_dag_module.dag_scheduler

    assert dag.dag_id == "debi_salesforce_glue_triggerer"
    assert dag.schedule_interval == "0 11,16,19 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    # start_date is timezone-aware (pendulum tz UTC)
    assert dag.start_date is not None
    assert str(dag.start_date.tzinfo) in ("UTC", "Timezone('UTC')", "UTC+00:00")

    # tags: includes your compatibility tag + failover tag
    assert "airflow-2.x.x-compatible" in dag.tags
    assert "failover-managed" in dag.tags


def test_trigger_task_exists_and_is_configured(loaded_dag_module):
    dag = loaded_dag_module.dag_scheduler

    # Only one active task in your screenshots (transformations is commented out)
    assert len(dag.tasks) == 1

    task = dag.get_task("trigger_salesforce_generic_dag")
    assert task is not None

    # Operator + trigger config
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    assert isinstance(task, TriggerDagRunOperator)
    assert task.trigger_dag_id == "debi_generic_ingester_glue_runner"
    assert task.wait_for_completion is True

    # Conf payload built with Variable.get() values (patched in fixture)
    expected_conf = {
        "vendor": "salesforce",
        "credentials": {
            "username": "test-user",
            "password": "test-pass",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        },
    }
    assert task.conf == expected_conf


def test_task_has_no_upstream_dependencies(loaded_dag_module):
    dag = loaded_dag_module.dag_scheduler
    task = dag.get_task("trigger_salesforce_generic_dag")

    assert task.upstream_task_ids == set()
    # downstream_task_ids empty too because transformations are commented out
    assert task.downstream_task_ids == set()
