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
    monkeypatch.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))


@pytest.fixture()
def loaded_dag_module(monkeypatch):
    """
    Patch Variable.get (and stub failover_managed_dag_tag if needed)
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

    # ---- Handle dags.common.dag_utilities.failover_managed_dag_tag ----
    # Prefer real module; only stub if import fails.
    util_mod_name = "dags.common.dag_utilities"
    try:
        importlib.import_module(util_mod_name)
    except Exception:
        stub = types.ModuleType(util_mod_name)

        def failover_managed_dag_tag():
            return "failover-managed"

        stub.failover_managed_dag_tag = failover_managed_dag_tag
        # setitem is reversible by monkeypatch (won't leak to other tests)
        monkeypatch.setitem(sys.modules, util_mod_name, stub)

    # Now load the DAG module (it will build the DAG immediately)
    module = _load_module_from_path(DAG_MODULE_NAME, dag_file)
    return module


def _get_schedule_value(dag):
    """
    Airflow versions can expose schedule via dag.schedule or dag.schedule_interval.
    Keep the assertion stable across 2.x variants.
    """
    if hasattr(dag, "schedule") and dag.schedule is not None:
        return str(dag.schedule)
    if hasattr(dag, "schedule_interval"):
        return str(dag.schedule_interval)
    return None


def test_dag_metadata(loaded_dag_module):
    dag = loaded_dag_module.dag_scheduler

    assert dag.dag_id == "debi_salesforce_glue_triggerer"
    assert _get_schedule_value(dag) == "0 11,16,19 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    assert dag.start_date is not None
    assert str(dag.start_date.tzinfo) in ("UTC", "Timezone('UTC')", "UTC+00:00")

    assert "airflow-2.x.x-compatible" in dag.tags
    # If you stubbed the tag helper, this will be present; if your real helper returns a different
    # string, adjust accordingly.
    assert "failover-managed" in dag.tags


def test_parallel_triggers_exist_and_are_configured(loaded_dag_module):
    dag = loaded_dag_module.dag_scheduler

    # Expect start + 2 triggers (and optionally join)
    assert "start" in dag.task_ids
    assert "trigger_salesforce_generic_dag" in dag.task_ids
    assert "trigger_salesforce_other_dag" in dag.task_ids

    from airflow.operators.empty import EmptyOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    start = dag.get_task("start")
    t1 = dag.get_task("trigger_salesforce_generic_dag")
    t2 = dag.get_task("trigger_salesforce_other_dag")

    assert isinstance(start, EmptyOperator)
    assert isinstance(t1, TriggerDagRunOperator)
    assert isinstance(t2, TriggerDagRunOperator)

    assert t1.trigger_dag_id == "debi_generic_ingester_glue_runner"
    # Update this if your second trigger uses a different DAG id
    assert isinstance(t2.trigger_dag_id, str) and len(t2.trigger_dag_id) > 0

    assert t1.wait_for_completion is True
    assert t2.wait_for_completion is True

    expected_conf = {
        "vendor": "salesforce",
        "credentials": {
            "username": "test-user",
            "password": "test-pass",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        },
    }
    assert t1.conf == expected_conf
    assert t2.conf == expected_conf


def test_triggers_run_in_parallel(loaded_dag_module):
    """
    Parallel means:
    - both triggers depend on 'start'
    - neither trigger depends on the other (no edge between them)
    """
    dag = loaded_dag_module.dag_scheduler

    t1 = dag.get_task("trigger_salesforce_generic_dag")
    t2 = dag.get_task("trigger_salesforce_other_dag")

    assert t1.upstream_task_ids == {"start"}
    assert t2.upstream_task_ids == {"start"}

    # No dependency between triggers
    assert "trigger_salesforce_other_dag" not in t1.upstream_task_ids
    assert "trigger_salesforce_other_dag" not in t1.downstream_task_ids
    assert "trigger_salesforce_generic_dag" not in t2.upstream_task_ids
    assert "trigger_salesforce_generic_dag" not in t2.downstream_task_ids


def test_optional_join_if_present(loaded_dag_module):
    """
    If you added a join task: start >> [t1, t2] >> join
    this test will validate it. If you didn't add join, it will just pass.
    """
    dag = loaded_dag_module.dag_scheduler

    if "join" not in dag.task_ids:
        pytest.skip("No join task in this DAG")

    join = dag.get_task("join")
    assert join.upstream_task_ids == {
        "trigger_salesforce_generic_dag",
        "trigger_salesforce_other_dag",
    }
