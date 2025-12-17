# tests/unit/test_salesforce_ingester_dag.py

from __future__ import annotations

import importlib
import json
import sys
import types
from typing import Any, Dict

import pytest

try:
    from airflow.models import DAG
except Exception:  # pragma: no cover
    DAG = object  # type: ignore

try:
    from airflow.models.baseoperator import BaseOperator
except Exception:  # pragma: no cover
    from airflow.models import BaseOperator  # type: ignore


DAG_MODULE = "dags.salesforce.salesforce_ingester"


class DummyGlueJobOperator(BaseOperator):
    """
    Minimal stand-in for AWS GlueJobOperator so the DAG can parse and we can
    assert init kwargs like script_args/wait_for_completion.
    """

    template_fields = ("script_args",)

    def __init__(
        self,
        *,
        job_name: str,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        script_args: Dict[str, Any] | None = None,
        wait_for_completion: bool | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.script_args = script_args or {}
        self.wait_for_completion = wait_for_completion


def _install_module(name: str, module: types.ModuleType) -> None:
    """
    Ensure parent packages exist in sys.modules, then install `name`.
    """
    parts = name.split(".")
    for i in range(1, len(parts)):
        pkg = ".".join(parts[:i])
        if pkg not in sys.modules:
            m = types.ModuleType(pkg)
            # mark as package
            m.__path__ = []  # type: ignore[attr-defined]
            sys.modules[pkg] = m
    sys.modules[name] = module


def _get_single_dag(mod) -> DAG:
    dags = [v for v in mod.__dict__.values() if isinstance(v, DAG)]
    assert dags, "No DAG object found in module"
    # In most DAG modules there is exactly one DAG instance
    return dags[0]


@pytest.fixture()
def dag_mod(monkeypatch):
    # ---- Stub your internal modules imported by the DAG ----
    dag_utils = types.ModuleType("dags.common.dag_utilities")
    dag_utils.failover_managed_dag_tag = lambda: "failover-managed"
    dag_utils.get_bucket_name = lambda env, region: f"bucket-{env}-{region}"
    dag_utils.get_c1s_oauth_endpoint = lambda env: "https://example.invalid/oauth"
    dag_utils.get_shairflow_environment = lambda: "dev"
    dag_utils.get_shairflow_region = lambda: "us-east-1"
    dag_utils.get_truncated_shairflow_region = lambda: "use1"
    _install_module("dags.common.dag_utilities", dag_utils)

    slack_mod = types.ModuleType("dags.common.slack")
    slack_mod.task_fail_slack_alert = lambda *args, **kwargs: None
    _install_module("dags.common.slack", slack_mod)

    udf_mod = types.ModuleType("dags.common.user_defined_filters")
    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = lambda s: s
    _install_module("dags.common.user_defined_filters", udf_mod)

    # ---- Stub GlueJobOperator import path used in the DAG ----
    glue_mod = types.ModuleType("airflow.providers.amazon.aws.operators.glue")
    glue_mod.GlueJobOperator = DummyGlueJobOperator
    _install_module("airflow.providers.amazon.aws.operators.glue", glue_mod)

    # ---- Patch Variable.get so DAG parse doesn't hit metadata DB ----
    from airflow.models import Variable

    fake_vars: Dict[str, Any] = {
        "INGESTER_TABLES": json.dumps(["Account", "Opportunity History"]),
        "INGESTER_ENV_VARS": json.dumps({"dev": {"FOO": "bar"}, "qa": {"FOO": "baz"}}),
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "qa",
        "INGESTER_VENDOR": "salesforce",
        "INGESTER_CONFIG_PATH": "some/config/path.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "CISCOREDATASERVICES_GITHUB_PASSWORD": "ghp_xxx",
        "INGESTER_START_DATE": "2000-01-01",
        "INGESTER_END_DATE": "2025-01-01",
        "C1SCOREDATASERVICES_EXCHANGE_ID": "ex_id",
        "C1SCOREDATASERVICES_EXCHANGE_SECRET": "ex_secret",
        "C1S_SALESFORCE_USERNAME": "sf_user",
        "C1S_SALESFORCE_PASSWORD": "sf_pass",
        "C1S_SALESFORCE_CLIENTID": "sf_client_id",
        "C1S_SALESFORCE_CLIENTSECRET": "sf_client_secret",
    }

    def fake_get(key: str, default_var: Any = None, **kwargs: Any) -> Any:
        return fake_vars.get(key, default_var)

    monkeypatch.setattr(Variable, "get", staticmethod(fake_get))

    # ---- OPTIONAL: tolerate `json.loads({ ...dict... })` if present ----
    # If your DAG accidentally uses json.loads(dict), this keeps tests from crashing.
    real_loads = json.loads

    def tolerant_loads(obj, *a, **k):
        if isinstance(obj, (dict, list)):
            return obj
        return real_loads(obj, *a, **k)

    monkeypatch.setattr(json, "loads", tolerant_loads)

    # ---- Import (or reload) the DAG module with patches in place ----
    if DAG_MODULE in sys.modules:
        mod = importlib.reload(sys.modules[DAG_MODULE])
    else:
        mod = importlib.import_module(DAG_MODULE)

    return mod


def test_dag_parses_and_has_expected_metadata(dag_mod):
    dag = _get_single_dag(dag_mod)

    assert dag.dag_id == "debi_ingester_glue_runner"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    # tags include your constants + the failover tag
    assert "invoke-lambda" in (dag.tags or [])
    assert "airflow-2.x.x-compatible" in (dag.tags or [])
    assert "failover-managed" in (dag.tags or [])

    # custom filter exists
    assert dag.user_defined_filters is not None
    assert "convertToEpochSeconds" in dag.user_defined_filters


def test_safe_task_id_helper(dag_mod):
    # _safe_task_id is defined in the DAG module
    assert dag_mod._safe_task_id("Opportunity History") == "opportunity-history"
    assert dag_mod._safe_task_id("A__B") == "a__b"
    assert dag_mod._safe_task_id("  Weird@@Name  ") == "weird-name"


def test_build_event_task_exists_and_is_upstream_of_glue_tasks(dag_mod):
    dag = _get_single_dag(dag_mod)
    task_ids = set(dag.task_ids)

    # your DAG uses XCom pull with task_ids='build_event' in script_args
    # (some variants might name it build_event_json)
    assert ("build_event" in task_ids) or ("build_event_json" in task_ids)

    # expected glue tasks for the two tables in fake vars
    assert "run_glue_job_account" in task_ids
    assert "run_glue_job_opportunity-history" in task_ids

    build_task_id = "build_event" if "build_event" in task_ids else "build_event_json"

    for glue_id in ("run_glue_job_account", "run_glue_job_opportunity-history"):
        glue_task = dag.get_task(glue_id)
        assert build_task_id in glue_task.upstream_task_ids


def test_glue_operator_has_expected_script_args_and_wait(dag_mod):
    dag = _get_single_dag(dag_mod)
    task_ids = set(dag.task_ids)
    build_task_id = "build_event" if "build_event" in task_ids else "build_event_json"

    glue_task = dag.get_task("run_glue_job_account")
    assert isinstance(glue_task, DummyGlueJobOperator)

    # operator wiring
    assert glue_task.job_name == "etl-job"
    assert glue_task.aws_conn_id == "etl-net-conn"
    assert glue_task.region_name == "us-east-1"
    assert glue_task.wait_for_completion is True

    # script args
    script_args = glue_task.script_args
    assert script_args["--env"] == "dev"
    assert script_args["--run_mode"] == "qa"
    assert script_args["--table"] == "Account"
    assert script_args["--vendor"] == "salesforce"
    assert script_args["--repo_name"] == "config_management"
    assert script_args["--file_path"] == "some/config/path.yml"
    assert script_args["--github_token"] == "ghp_xxx"
    assert script_args["--start_date"] == "2000-01-01"
    assert script_args["--end_date"] == "2025-01-01"

    # event payload comes from XCom
    assert script_args["--event"] == f"{{{{ ti.xcom_pull(task_ids='{build_task_id}') }}}}"
