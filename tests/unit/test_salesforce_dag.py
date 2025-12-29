from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, mock_open

import pytest


@pytest.fixture
def mod(monkeypatch):
    """
    Import the DAG module under test with required env set.
    Adjust the import path if your project structure differs.
    """
    # Needed by your module: CURRENT_DIR = os.environ["AIRFLOW__CORE__DAGS_FOLDER"] + "/dags/salesforce"
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow")

    # Import after env var is set
    import dags.salesforce.salesforce_ingester as m  # <-- adjust if needed

    return m


def test_get_copy_sql_strips_bucket_and_replaces_tokens(monkeypatch, mod):
    # SQL template file contents
    tpl = (
        "COPY INTO {{ params.target_table }}\n"
        "FROM @VALIDATION_STAGE/{{ params.s3_uri }}\n"
    )

    # Mock open() used inside get_copy_sql
    m = mock_open(read_data=tpl)
    monkeypatch.setattr("builtins.open", m)

    # IMPORTANT: call the TaskFlow python callable
    out = mod.get_copy_sql.python_callable(
        table_name="VALIDATION.PUBLIC.ACCOUNT",
        s3_uri="s3://my-bucket/test/salesforce/account/year=2025/month=12/file.parquet",
    )

    # bucket removed => starts at key
    assert "test/salesforce/account/year=2025/month=12/file.parquet" in out
    assert "s3://my-bucket/" not in out

    # placeholders replaced
    assert "COPY INTO VALIDATION.PUBLIC.ACCOUNT" in out
    assert (
        "FROM @VALIDATION_STAGE/test/salesforce/account/year=2025/month=12/file.parquet"
        in out
    )

    # file opened at expected path (rough check)
    # file_name = f"{CURRENT_DIR}/copy/copy_{table_name}.sql"
    m.assert_called_once()
    called_path = m.call_args[0][0]
    assert called_path.endswith(
        "/dags/salesforce/copy/copy_VALIDATION.PUBLIC.ACCOUNT.sql"
    )


def test_get_latest_s3_uri_returns_newest_matching_parquet(monkeypatch, mod):
    # Build a fake paginator response with multiple objects
    pages = [
        {
            "Contents": [
                {
                    "Key": "test/salesforce/account/account-20251229T150938Z.parquet",
                    "LastModified": datetime(
                        2025, 12, 29, 15, 9, 40, tzinfo=timezone.utc
                    ),
                },
                {
                    "Key": "test/salesforce/account/account-20251229T150227Z.parquet",
                    "LastModified": datetime(
                        2025, 12, 29, 15, 2, 29, tzinfo=timezone.utc
                    ),
                },
                # non-matching extension should be ignored when pattern is "*.parquet"
                {
                    "Key": "test/salesforce/account/_SUCCESS",
                    "LastModified": datetime(
                        2025, 12, 29, 15, 10, 0, tzinfo=timezone.utc
                    ),
                },
            ]
        }
    ]

    paginator = MagicMock()
    paginator.paginate.return_value = pages

    s3_client = MagicMock()
    s3_client.get_paginator.return_value = paginator

    monkeypatch.setattr(mod.boto3, "client", MagicMock(return_value=s3_client))

    out = mod.get_latest_s3_uri.python_callable(
        s3_prefix="s3://my-bucket/test/salesforce/account/",
        pattern="*.parquet",
    )

    assert (
        out
        == "s3://my-bucket/test/salesforce/account/account-20251229T150938Z.parquet"
    )


def test_salesforce_ingester_dag_builds_expected_tasks(monkeypatch, mod):
    """
    This validates:
      - task ids exist for each table
      - dependencies are wired as expected
    """

    # Stub out helper functions imported from dags.common.dag_utilities
    monkeypatch.setattr(mod, "get_shairflow_environment", lambda: "qa")
    monkeypatch.setattr(mod, "get_shairflow_region", lambda: "us-east-1")
    monkeypatch.setattr(
        mod, "get_truncated_shairflow_region", lambda: "us-east-1"
    )
    monkeypatch.setattr(mod, "get_bucket_name", lambda env, region: "my-bucket")
    monkeypatch.setattr(
        mod, "get_c1s_oauth_endpoint", lambda env: "https://example/token"
    )
    monkeypatch.setattr(mod, "failover_managed_dag_tag", lambda: "failover-tag")

    # Variable.get needs to return a dict (your code does Variable.get(..., deserialize_json=True))
    workflow = {
        "INGESTER_TABLES": [
            "Account",
            "Opportunity",
        ],  # mixed case to test safe_task_id()
        "INGESTER_RUN_MODE": "once",
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_CONFIG_PATH": "cfg/path.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "INGESTER_PYTHON_MODULE": "some.module",
        "INGESTER_START_DATE": "2000-01-01",
        "INGESTER_END_DATE": "2000-01-02",
        "INGESTER_ENV_VARS": {},
        "INGESTER_SQL_PARAMS": {"DATABASE": "VALIDATION", "SCHEMA": "PUBLIC"},
    }

    def fake_variable_get(key, default_var=None, deserialize_json=False):
        if key.startswith("INGESTER_WORKFLOW_"):
            return workflow
        # any other Variable.get(...) calls in your DAG can safely return default
        return default_var

    monkeypatch.setattr(mod.Variable, "get", fake_variable_get)

    dag = mod.salesforce_ingester_dag()
    assert dag.dag_id == "debi_ingester_glue_runner"

    # safe_task_id makes these lower-case
    for safe in ("account", "opportunity"):
        assert f"build_event_{safe}" in dag.task_ids
        assert f"run_glue_job_{safe}" in dag.task_ids
        assert f"latest_parquet_{safe}" in dag.task_ids
        assert f"copy_sql_{safe}" in dag.task_ids
        assert f"load_table_{safe}" in dag.task_ids

        build_event = dag.get_task(f"build_event_{safe}")
        run_glue = dag.get_task(f"run_glue_job_{safe}")
        latest_parquet = dag.get_task(f"latest_parquet_{safe}")
        copy_sql = dag.get_task(f"copy_sql_{safe}")
        load_table = dag.get_task(f"load_table_{safe}")

        # Dependencies you show in code:
        # latest_zip >> build_event >> run_glue
        # run_glue >> latest_parquet >> copy_sql >> load_table
        assert run_glue.task_id in build_event.downstream_task_ids
        assert latest_parquet.task_id in run_glue.downstream_task_ids
        assert copy_sql.task_id in latest_parquet.downstream_task_ids
        assert load_table.task_id in copy_sql.downstream_task_ids
