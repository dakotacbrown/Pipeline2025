from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import boto3
import pytest


@pytest.fixture
def workflow_var_dict():
    # Minimal workflow dict needed to build the DAG for tests
    return {
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "once",
        "INGESTER_TABLES": ["Opportunity"],
        "INGESTER_PYTHON_MODULE": "c1-asvc1scoredataservices-common==0.1.41",
        "INGESTER_CONFIG_PATH": "ingester/salesforce.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "INGESTER_START_DATE": "2000-01-01",
        "INGESTER_END_DATE": "2000-01-31",
        "INGESTER_ENV_VARS": {"X_UPSTREAM_ENV": "capitalonesoftware-qa"},
        "INGESTER_SQL_PARAMS": {
            "DATABASE": "VALIDATION",
            "SCHEMA": "PUBLIC",
        },
    }


@pytest.fixture
def copy_sql_template():
    # This should match the new SQL template that uses params.s3_uri
    return """\
COPY INTO {{ params.target_table }}
FROM '{{ params.s3_uri }}'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
"""


def _mock_s3_listing(
    monkeypatch, *, bucket: str, keys_in_order_old_to_new: list[str]
):
    """
    Patch boto3.client("s3") paginator to return Contents with LastModified timestamps.
    keys_in_order_old_to_new: keys where last one should be selected as newest.
    """
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator

    contents = []
    for idx, key in enumerate(keys_in_order_old_to_new):
        contents.append(
            {
                "Key": key,
                "LastModified": datetime(2025, 1, 1 + idx, tzinfo=timezone.utc),
            }
        )

    mock_paginator.paginate.return_value = [{"Contents": contents}]
    monkeypatch.setattr(boto3, "client", lambda *_args, **_kwargs: mock_s3)


def _install_dag_utilities_stub(monkeypatch):
    """
    Provide the imported helpers from dags.common.dag_utilities so DAG import works.
    """
    dag_utils = ModuleType("dags.common.dag_utilities")

    dag_utils.failover_managed_dag_tag = lambda: "failover"
    dag_utils.get_bucket_name = lambda env, truncated_region: "my-bucket"
    dag_utils.get_c1s_oauth_endpoint = lambda env: "https://oauth.example/token"
    dag_utils.get_shairflow_environment = lambda: "qa"
    dag_utils.get_shairflow_region = lambda: "us-east-1"
    dag_utils.get_truncated_shairflow_region = lambda: "use1"

    monkeypatch.setitem(sys.modules, "dags.common.dag_utilities", dag_utils)

    slack_mod = ModuleType("dags.common.slack")
    slack_mod.task_fail_slack_alert = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "dags.common.slack", slack_mod)

    udf_mod = ModuleType("dags.common.user_defined_filters")
    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = lambda v: str(v)
    monkeypatch.setitem(
        sys.modules, "dags.common.user_defined_filters", udf_mod
    )


def _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template):
    """
    Import/reload the DAG module with Variable.get mocked and utility modules stubbed.
    """
    _install_dag_utilities_stub(monkeypatch)

    # Import once so we can patch its Variable reference then reload.
    import dags.salesforce.salesforce_ingester as mod  # noqa: F401

    def fake_variable_get(key, default_var=None, deserialize_json=False):
        if key == "INGESTER_COPY_SQL":
            return copy_sql_template
        if key == "INGESTER_WORKFLOW_SALESFORCE":
            # Your code uses deserialize_json=True for this key, so return dict
            return workflow_var_dict
        return default_var

    # Patch Variable used inside the module
    monkeypatch.setattr(
        mod, "Variable", SimpleNamespace(get=fake_variable_get), raising=True
    )

    # Reload so patched Variable is used during DAG creation (module-level dag = ...)
    mod = importlib.reload(mod)
    return mod


def test_get_latest_s3_uri_picks_newest(monkeypatch):
    # Prepare fake listing
    bucket = "my-bucket"
    prefix_key_old = "data/qa/use1/salesforce/Opportunity/file1.parquet"
    prefix_key_new = "data/qa/use1/salesforce/Opportunity/file2.parquet"

    _mock_s3_listing(
        monkeypatch,
        bucket=bucket,
        keys_in_order_old_to_new=[prefix_key_old, prefix_key_new],
    )

    from dags.salesforce.salesforce_ingester import get_latest_s3_uri

    result = get_latest_s3_uri.function(
        s3_prefix=f"s3://{bucket}/data/qa/use1/salesforce/Opportunity/",
        pattern="*.parquet",
    )
    assert result == f"s3://{bucket}/{prefix_key_new}"


def test_dag_builds_expected_tasks(
    monkeypatch, workflow_var_dict, copy_sql_template
):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    assert dag.dag_id == "debi_ingester_glue_runner"

    # Shared zip task
    assert "latest_framework_zip" in dag.task_ids

    # Table=Opportunity -> safe task id = "opportunity"
    assert "build_event_opportunity" in dag.task_ids
    assert "run_glue_job_opportunity" in dag.task_ids
    assert "latest_parquet_opportunity" in dag.task_ids
    assert "load_table_opportunity" in dag.task_ids


def test_load_table_sql_uses_params_s3_uri(
    monkeypatch, workflow_var_dict, copy_sql_template
):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    load = dag.get_task("load_table_opportunity")

    assert "{{ params.s3_uri }}" in load.sql
    # ensure we are NOT doing glue meta xcom inside SQL anymore
    assert "ti.xcom_pull(task_ids=params.glue_task_id)" not in load.sql
    assert "meta" not in load.sql  # should not reference meta at all


def test_load_table_params_pull_latest_parquet_task(
    monkeypatch, workflow_var_dict, copy_sql_template
):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    load = dag.get_task("load_table_opportunity")

    assert load.params["target_table"] == "VALIDATION.PUBLIC.Opportunity"
    assert (
        load.params["s3_uri"]
        == "{{ ti.xcom_pull(task_ids='latest_parquet_opportunity') }}"
    )


def test_dependencies(monkeypatch, workflow_var_dict, copy_sql_template):
    mod = _import_dag_module(monkeypatch, workflow_var_dict, copy_sql_template)
    dag = mod.dag

    latest_zip = dag.get_task("latest_framework_zip")
    build_event = dag.get_task("build_event_opportunity")
    run_glue = dag.get_task("run_glue_job_opportunity")
    latest_parquet = dag.get_task("latest_parquet_opportunity")
    load = dag.get_task("load_table_opportunity")

    # zip -> build_event -> run_glue
    assert latest_zip.task_id in {t.task_id for t in build_event.upstream_list}
    assert build_event.task_id in {t.task_id for t in run_glue.upstream_list}

    # load depends on both glue + latest parquet
    upstream_ids = {t.task_id for t in load.upstream_list}
    assert run_glue.task_id in upstream_ids
    assert latest_parquet.task_id in upstream_ids

    # sanity: load should NOT be upstream of run_glue
    assert load.task_id not in {t.task_id for t in run_glue.upstream_list}
