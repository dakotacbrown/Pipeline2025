from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

DAG_FILE_RELATIVE_PATH = Path("dags/generic/salesforce_triggerer.py")
DAG_MODULE_NAME = "salesforce_triggerer_under_test"


def _project_root() -> Path:
    # Assumes tests live in: <repo>/tests/unit/test_*.py
    return Path(__file__).resolve().parents[2]


def _load_module_from_path(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module spec from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _install_stub_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Stub dags.common.dag_utilities so importing the DAG doesn't require your full
    dags.common package during unit tests.
    """
    dags_mod = types.ModuleType("dags")
    common_mod = types.ModuleType("dags.common")
    dag_utils_mod = types.ModuleType("dags.common.dag_utilities")

    def failover_managed_dag_tag() -> str:
        return "failover-managed"

    dag_utils_mod.failover_managed_dag_tag = failover_managed_dag_tag

    monkeypatch.setitem(sys.modules, "dags", dags_mod)
    monkeypatch.setitem(sys.modules, "dags.common", common_mod)
    monkeypatch.setitem(sys.modules, "dags.common.dag_utilities", dag_utils_mod)


@pytest.fixture()
def dag_module(monkeypatch: pytest.MonkeyPatch):
    _install_stub_modules(monkeypatch)

    # Patch Variable.get to avoid DB/metastore access at import/parse time
    from airflow.models import Variable

    def _fake_get(key: str, default=None):
        return f"val::{key}"

    monkeypatch.setattr(Variable, "get", staticmethod(_fake_get), raising=True)

    dag_path = _project_root() / DAG_FILE_RELATIVE_PATH
    return _load_module_from_path(DAG_MODULE_NAME, dag_path)


def test_dag_object_exists(dag_module):
    assert hasattr(dag_module, "dag_scheduler")
    dag = dag_module.dag_scheduler
    assert dag is not None
    assert getattr(dag, "dag_id", None) == "debi_salesforce_glue_triggerer"


def test_dag_metadata(dag_module):
    dag = dag_module.dag_scheduler

    # schedule can be stored differently depending on Airflow minor versions
    schedule = getattr(dag, "schedule_interval", None)
    if schedule is None:
        schedule = getattr(dag, "schedule", None)
    assert schedule is not None

    # doc_md should include your header line
    assert "salesforce_scheduler DAG triggers" in (dag.doc_md or "")

    # tags should include both the compatibility tag and our stubbed failover tag
    assert "airflow-2.x.x-compatible" in (dag.tags or [])
    assert "failover-managed" in (dag.tags or [])


def test_dag_has_trigger_task_with_expected_config(dag_module):
    dag = dag_module.dag_scheduler

    task = dag.get_task("trigger_salesforce_generic_dag")
    assert task is not None

    # TriggerDagRunOperator fields
    assert (
        getattr(task, "trigger_dag_id", None)
        == "debi_generic_ingester_glue_runner"
    )
    assert getattr(task, "wait_for_completion", None) is True

    conf = getattr(task, "conf", None)
    assert isinstance(conf, dict)
    assert conf.get("vendor") == "salesforce"

    creds = conf.get("credentials")
    assert isinstance(creds, dict)

    assert creds["username"] == "val::C1S_SALESFORCE_USERNAME"
    assert creds["password"] == "val::C1S_SALESFORCE_PASSWORD"
    assert creds["client_id"] == "val::C1S_SALESFORCE_CLIENTID"
    assert creds["client_secret"] == "val::C1S_SALESFORCE_CLIENTSECRET"


def test_only_one_task_in_dag(dag_module):
    dag = dag_module.dag_scheduler
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == ["trigger_salesforce_generic_dag"]
