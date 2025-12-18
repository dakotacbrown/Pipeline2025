from __future__ import annotations

import importlib
import json
import sys
import types
from typing import Any, Dict, List

import pytest
from airflow.models import Variable

# Update if your module path differs
DAG_MODULE = "dags.salesforce.salesforce_ingester"


class DummyGlueJobOperator:
    """
    Lightweight stand-in for GlueJobOperator so the DAG can parse without AWS provider
    behavior and so we can inspect init kwargs.

    Airflow will only require:
      - task_id attribute
      - upstream/downstream wiring via BaseOperator methods IF you chain tasks
    But in TaskFlow DAGs, chaining is done on real BaseOperator objects.
    So we implement as a real BaseOperator subclass.
    """

    pass


@pytest.fixture(scope="session")
def _airflow_baseoperator():
    # Import lazily so tests still collect even if airflow import is slow
    from airflow.models.baseoperator import BaseOperator

    return BaseOperator


def _install_module(name: str, module: types.ModuleType) -> None:
    """Install stub module into sys.modules (creating parents as packages)."""
    parts = name.split(".")
    for i in range(1, len(parts)):
        pkg = ".".join(parts[:i])
        if pkg not in sys.modules:
            m = types.ModuleType(pkg)
            m.__path__ = []  # mark as pkg
            sys.modules[pkg] = m
    sys.modules[name] = module


@pytest.fixture()
def dag_mod(monkeypatch, _airflow_baseoperator):
    # ----- Stub internal imports used by the DAG -----
    dag_utils = types.ModuleType("dags.common.dag_utilities")
    dag_utils.failover_managed_dag_tag = lambda: "failover-managed"
    dag_utils.get_bucket_name = (
        lambda env, truncated_region: f"bucket-{env}-{truncated_region}"
    )
    dag_utils.get_c1s_oauth_endpoint = (
        lambda env: "https://example.invalid/oauth"
    )
    dag_utils.get_shairflow_environment = lambda: "dev"
    dag_utils.get_shairflow_region = lambda: "us-east-1"
    dag_utils.get_truncated_shairflow_region = lambda: "use1"
    _install_module("dags.common.dag_utilities", dag_utils)

    slack_mod = types.ModuleType("dags.common.slack")
    slack_mod.task_fail_slack_alert = lambda *a, **k: None
    _install_module("dags.common.slack", slack_mod)

    udf_mod = types.ModuleType("dags.common.user_defined_filters")
    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = lambda s: s
    _install_module("dags.common.user_defined_filters", udf_mod)

    # ----- Stub GlueJobOperator import path used by the DAG -----
    class StubGlueJobOperator(_airflow_baseoperator):
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

    glue_mod = types.ModuleType("airflow.providers.amazon.aws.operators.glue")
    glue_mod.GlueJobOperator = StubGlueJobOperator
    _install_module("airflow.providers.amazon.aws.operators.glue", glue_mod)

    # ----- Patch Variable.get (DAG reads vars at parse time) -----
    fake_vars: Dict[str, Any] = {
        "INGESTER_TABLES": json.dumps(["Account", "Opportunity History"]),
        "INGESTER_VENDOR": "salesforce",
        "INGESTER_CONFIG_PATH": "some/config/path.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "CISCOREDATASERVICES_GITHUB_PASSWORD": "ghp_xxx",
        "INGESTER_START_DATE": "2000-01-01",
        "INGESTER_END_DATE": "2025-01-01",
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "qa",
        "INGESTER_ENV_VARS": json.dumps(
            {"dev": {"FOO": "bar"}, "qa": {"FOO": "baz"}}
        ),
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

    # ----- Import/reload module under test -----
    if DAG_MODULE in sys.modules:
        mod = importlib.reload(sys.modules[DAG_MODULE])
    else:
        mod = importlib.import_module(DAG_MODULE)

    # Expose StubGlueJobOperator for isinstance checks in tests
    mod._StubGlueJobOperator = StubGlueJobOperator  # type: ignore[attr-defined]
    return mod


def test_safe_task_id(dag_mod):
    assert dag_mod._safe_task_id("Opportunity History") == "opportunity-history"
    assert dag_mod._safe_task_id("Account") == "account"
    assert dag_mod._safe_task_id("a__b") == "a__b"


def test_dag_metadata(dag_mod):
    dag = dag_mod.dag
    assert dag.dag_id == "debi_ingester_glue_runner"
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    assert "invoke-lambda" in (dag.tags or [])
    assert "airflow-2.x.x-compatible" in (dag.tags or [])
    assert "failover-managed" in (dag.tags or [])

    assert dag.user_defined_filters is not None
    assert "convertToEpochSeconds" in dag.user_defined_filters


def test_per_table_tasks_exist(dag_mod):
    dag = dag_mod.dag
    task_ids = set(dag.task_ids)

    # tables are ["Account", "Opportunity History"] per fake vars
    assert "build_event__account" in task_ids
    assert "run_glue_job__account" in task_ids

    assert "build_event__opportunity-history" in task_ids
    assert "run_glue_job__opportunity-history" in task_ids


def test_dependencies_build_event_to_glue_per_table(dag_mod):
    dag = dag_mod.dag

    be_account = dag.get_task("build_event__account")
    glue_account = dag.get_task("run_glue_job__account")
    assert glue_account.task_id in be_account.downstream_task_ids
    assert be_account.task_id in glue_account.upstream_task_ids

    be_opp = dag.get_task("build_event__opportunity-history")
    glue_opp = dag.get_task("run_glue_job__opportunity-history")
    assert glue_opp.task_id in be_opp.downstream_task_ids
    assert be_opp.task_id in glue_opp.upstream_task_ids


def test_glue_operator_args_and_event_template(dag_mod):
    dag = dag_mod.dag
    StubGlueJobOperator = dag_mod._StubGlueJobOperator  # type: ignore[attr-defined]

    glue = dag.get_task("run_glue_job__account")
    assert isinstance(glue, StubGlueJobOperator)

    assert glue.job_name == "etl-job"
    assert glue.aws_conn_id == "etl-net-conn"
    assert glue.region_name == "us-east-1"
    assert glue.wait_for_completion is True

    args = glue.script_args
    # core args you showed
    assert args["--env"] == "dev"
    assert args["--run_mode"] == "qa"
    assert args["--table"] == "Account"
    assert args["--vendor"] == "salesforce"
    assert args["--repo_name"] == "config_management"
    assert args["--file_path"] == "some/config/path.yml"
    assert args["--github_token"] == "ghp_xxx"
    assert args["--start_date"] == "2000-01-01"
    assert args["--end_date"] == "2025-01-01"

    # Matches your screenshot exactly (even though it doesn’t reference the per-table build_event id)
    assert args["--event"] == "{{ ti.xcom_pull(task_ids='build_event') }}"
