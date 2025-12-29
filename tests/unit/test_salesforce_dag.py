# tests/unit/test_salesforce_ingester_dag.py

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock

# --------------------------------------------------------------------------------------
# Import the module under test
# --------------------------------------------------------------------------------------
# CHANGE THIS to your real module path (based on your screenshots it looks like this):
# dags/salesforce/salesforce_ingester.py
import dags.salesforce.salesforce_ingester as mod  # <-- CHANGE THIS if needed
import pytest


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _dt(y: int, m: int, d: int, hh: int, mm: int, ss: int) -> datetime:
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


# --------------------------------------------------------------------------------------
# Unit tests
# --------------------------------------------------------------------------------------
def test_safe_task_id_sanitizes() -> None:
    assert mod._safe_task_id("Account") == "account"
    assert mod._safe_task_id("Account Name") == "account-name"
    assert mod._safe_task_id("Account__Name!!") == "account__name"
    assert mod._safe_task_id("__Account--") == "account"


def test_get_latest_s3_uri_picks_newest_and_filters_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    - Lists objects under prefix
    - If pattern is given, match is done on filename only (not full key)
    - Ignores non-matching extensions
    - Returns s3://bucket/key for newest matching object
    """
    pages: List[Dict[str, Any]] = [
        {
            "Contents": [
                {
                    "Key": "test/salesforce/account/account-20251229T150938Z.parquet",
                    "LastModified": _dt(2025, 12, 29, 15, 9, 40),
                },
                {
                    "Key": "test/salesforce/account/account-20251229T150227Z.parquet",
                    "LastModified": _dt(2025, 12, 29, 15, 2, 29),
                },
                # should be ignored when pattern="*.parquet"
                {
                    "Key": "test/salesforce/account/_SUCCESS",
                    "LastModified": _dt(2025, 12, 29, 15, 10, 0),
                },
            ]
        }
    ]

    paginator = MagicMock()
    paginator.paginate.return_value = pages

    s3_client = MagicMock()
    s3_client.get_paginator.return_value = paginator

    monkeypatch.setattr(mod.boto3, "client", MagicMock(return_value=s3_client))

    out = mod.get_latest_s3_uri.function(
        s3_prefix="s3://my-bucket/test/salesforce/account/",
        pattern="*.parquet",
    )

    assert (
        out
        == "s3://my-bucket/test/salesforce/account/account-20251229T150938Z.parquet"
    )

    # sanity: ensure it called list_objects_v2 paginate with Bucket + Prefix
    paginator.paginate.assert_called()
    kwargs = paginator.paginate.call_args.kwargs
    assert kwargs["Bucket"] == "my-bucket"
    assert kwargs["Prefix"] == "test/salesforce/account/"


def test_get_latest_s3_uri_raises_when_no_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [
        {
            "Contents": [
                {
                    "Key": "test/salesforce/account/_SUCCESS",
                    "LastModified": _dt(2025, 12, 29, 15, 10, 0),
                }
            ]
        }
    ]

    paginator = MagicMock()
    paginator.paginate.return_value = pages

    s3_client = MagicMock()
    s3_client.get_paginator.return_value = paginator

    monkeypatch.setattr(mod.boto3, "client", MagicMock(return_value=s3_client))

    with pytest.raises(ValueError):
        mod.get_latest_s3_uri.function(
            s3_prefix="s3://my-bucket/test/salesforce/account/",
            pattern="*.parquet",
        )


def test_get_copy_sql_reads_template_and_strips_bucket(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Ensures:
      - reads copy template from CURRENT_DIR/copy/copy_{table}.sql
      - replaces {{ params.target_table }} and {{ params.s3_uri }}
      - strips bucket from s3_uri before substituting (keeps key/path only)
    """
    # Create fake dags dir layout
    dags_dir = tmp_path / "dags" / "salesforce"
    copy_dir = dags_dir / "copy"
    copy_dir.mkdir(parents=True)

    # Template similar to your screenshot usage
    template = (
        "DELETE FROM {{ params.target_table }};\n"
        "COPY INTO {{ params.target_table }}\n"
        "FROM @VALIDATION_STAGE/{{ params.s3_uri }}\n"
        "FILE_FORMAT=(TYPE=PARQUET);\n"
    )
    (copy_dir / "copy_ACCOUNT.sql").write_text(template)

    # Make module use this directory
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", str(tmp_path))
    # Update CURRENT_DIR in module to match new env
    mod.CURRENT_DIR = (
        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] + "/dags/salesforce"
    )

    sql = mod.get_copy_sql.function(
        table_name="ACCOUNT",
        s3_uri="s3://my-bucket/test/salesforce/account/account-20251229T150938Z.parquet",
    )

    assert (
        "validation.public.ACCOUNT" not in sql
    )  # we didn't pass that here; just sanity
    assert (
        "DELETE FROM ACCOUNT" in sql or "{{ params.target_table }}" not in sql
    )

    # bucket stripped: we expect only the key/path substituted
    assert (
        "FROM @VALIDATION_STAGE/test/salesforce/account/account-20251229T150938Z.parquet"
        in sql
    )
    assert "s3://my-bucket" not in sql
    assert "{{ params.s3_uri }}" not in sql
    assert "{{ params.target_table }}" not in sql


def test_salesforce_ingester_dag_builds_expected_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Builds the DAG and asserts tasks exist & dependencies match your code:
      latest_zip >> build_event >> run_glue
      run_glue >> latest_parquet >> copy_sql >> load_table

    We monkeypatch:
      - Variable.get to provide config
      - env/region helpers
      - operators to avoid real provider behavior
    """
    # -------------------------
    # Patch Variable.get config
    # -------------------------
    workflow = {
        "INGESTER_TABLES": ["ACCOUNT"],
        "INGESTER_RUN_MODE": "once",
        "INGESTER_CONFIG_PATH": "salesforce/qa/tables/v1.3",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "INGESTER_START_DATE": "2020-01-01",
        "INGESTER_END_DATE": "2020-01-02",
        "INGESTER_ENV_VARS": {},
        "INGESTER_PYTHON_MODULE": None,
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_SQL_PARAMS": {"DATABASE": "VALIDATION", "SCHEMA": "PUBLIC"},
    }

    def fake_variable_get(key: str, default_var=None, deserialize_json=False):
        if key == "INGESTER_WORKFLOW_SALESFORCE":
            return workflow
        # github password etc
        return default_var

    monkeypatch.setattr(mod.Variable, "get", staticmethod(fake_variable_get))

    # -------------------------
    # Patch helper funcs
    # -------------------------
    monkeypatch.setattr(mod, "get_shairflow_environment", lambda: "qa")
    monkeypatch.setattr(mod, "get_shairflow_region", lambda: "us-east-1")
    monkeypatch.setattr(mod, "get_truncated_shairflow_region", lambda: "use1")
    monkeypatch.setattr(mod, "get_bucket_name", lambda env, region: "my-bucket")
    monkeypatch.setattr(mod, "failover_managed_dag_tag", lambda: "managed")

    # -------------------------
    # Patch Operators/Tasks that are provider-heavy
    # -------------------------
    # GlueJobOperator and SQLExecuteQueryOperator can be used as-is, but we patch to simple stand-ins
    class DummyOperator:
        def __init__(self, task_id: str, **kwargs):
            self.task_id = task_id
            self.kwargs = kwargs
            self.downstream_task_ids = set()

        def __rshift__(self, other):
            self.downstream_task_ids.add(other.task_id)
            return other

    monkeypatch.setattr(mod, "GlueJobOperator", DummyOperator)
    monkeypatch.setattr(mod, "SQLExecuteQueryOperator", DummyOperator)

    # If your DAG imports slack callback etc, it's fine, but keep it from failing on callability:
    monkeypatch.setattr(
        mod, "task_fail_slack_alert", lambda *args, **kwargs: None
    )

    # -------------------------
    # Build DAG
    # -------------------------
    dag = mod.salesforce_ingester_dag()

    safe = mod._safe_task_id("ACCOUNT")

    # Task IDs
    expected = {
        "latest_framework_zip",
        f"build_event_{safe}",
        f"run_glue_job_{safe}",
        f"latest_parquet_{safe}",
        f"copy_sql_{safe}",
        f"load_table_{safe}",
    }

    assert expected.issubset(set(dag.task_ids))

    latest_zip = dag.get_task("latest_framework_zip")
    build_event = dag.get_task(f"build_event_{safe}")
    run_glue = dag.get_task(f"run_glue_job_{safe}")
    latest_parquet = dag.get_task(f"latest_parquet_{safe}")
    copy_sql = dag.get_task(f"copy_sql_{safe}")
    load_table = dag.get_task(f"load_table_{safe}")

    # Dependencies per your comments in code
    assert build_event.task_id in latest_zip.downstream_task_ids
    assert run_glue.task_id in build_event.downstream_task_ids
    assert latest_parquet.task_id in run_glue.downstream_task_ids
    assert copy_sql.task_id in latest_parquet.downstream_task_ids
    assert load_table.task_id in copy_sql.downstream_task_ids
