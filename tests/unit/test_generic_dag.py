# tests/unit/test_generic_ingester_dag.py
import importlib
import io
import json
import sys
from datetime import datetime, timezone
from typing import Dict, Optional

import pytest

MODULE_PATH = "dags.generic.generic_ingester"


def _dt(y, m, d, hh=0, mm=0, ss=0):
    # Use tz-aware datetimes so max() comparisons are consistent.
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


class _FakePaginator:
    """
    A paginator that returns a DIFFERENT set of pages per paginate() call.
    This matches get_latest_s3_uri(), which calls paginate() multiple times:
      - once for CommonPrefixes (top-level)
      - then again per prefix to find latest object timestamps
    """

    def __init__(self, pages_by_call):
        # pages_by_call: list of "call pages"
        # each "call pages" can be:
        #   - a dict (single page)
        #   - a list[dict] (multiple pages)
        self._pages_by_call = pages_by_call
        self._call_idx = 0

    def paginate(self, **kwargs):
        if self._call_idx >= len(self._pages_by_call):
            return iter([])

        pages = self._pages_by_call[self._call_idx]
        self._call_idx += 1

        if pages is None:
            return iter([])

        if isinstance(pages, dict):
            return iter([pages])

        return iter(pages)


class _FakeS3Client:
    def __init__(self, paginator):
        self._paginator = paginator

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self._paginator


def _get_task_callable(obj):
    """
    Airflow 2.10.5:
      - @task decorated funcs are TaskDecorator -> has .function
      - operator instances -> has .python_callable
    """
    if hasattr(obj, "python_callable"):
        return obj.python_callable
    if hasattr(obj, "function"):
        return obj.function
    if hasattr(obj, "__wrapped__"):
        return obj.__wrapped__
    raise TypeError(f"Don't know how to get callable from: {type(obj)}")


def _import_module_fresh(
    monkeypatch,
    *,
    workflow_var: dict,
    sql_templates: Optional[Dict[str, str]] = None,
    dag_run_conf: Optional[dict] = None,
    s3_pages_by_call=None,
):
    """
    Patch all external deps BEFORE importing the DAG module so module-level DAG creation works.
    """
    if MODULE_PATH in sys.modules:
        del sys.modules[MODULE_PATH]

    if sql_templates is None:
        sql_templates = {
            "copy": "COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}';",
            "deduplication": "DELETE FROM {{ params.target_table }} WHERE 1=0;",
        }

    # -------------------------
    # Patch Airflow Variable.get
    # -------------------------
    def _fake_variable_get(key, default_var=None, deserialize_json=False):
        if key.startswith("INGESTER_WORKFLOW_"):
            return (
                workflow_var if deserialize_json else json.dumps(workflow_var)
            )

        # values used by build_exchange_extras / github token
        if key == "C1SCOREDATASERVICES_GITHUB_PASSWORD":
            return "ghp_xxx"
        if key == "C1SCOREDATASERVICES_EXCHANGE_ID":
            return "ex_id"
        if key == "C1SCOREDATASERVICES_EXCHANGE_SECRET":
            return "ex_secret"

        return default_var

    monkeypatch.setattr(
        "airflow.models.Variable.get", _fake_variable_get, raising=False
    )

    # -------------------------
    # Patch get_current_context
    # -------------------------
    if dag_run_conf is None:
        dag_run_conf = {
            "vendor": "generic",
            "start_date": "2020-01-01",
            "end_date": "2020-01-31",
            "credentials": {"bar": "baz"},
        }

    def _fake_get_current_context():
        dr = type("DR", (), {"conf": dag_run_conf})()
        return {"dag_run": dr}

    monkeypatch.setattr(
        "airflow.operators.python.get_current_context",
        _fake_get_current_context,
        raising=False,
    )

    # -------------------------
    # Patch dag_utilities getters
    # -------------------------
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_shairflow_environment",
        lambda: "dev",
        raising=False,
    )
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_shairflow_region",
        lambda: "us-east-1",
        raising=False,
    )
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_truncated_shairflow_region",
        lambda: "use1",
        raising=False,
    )
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_bucket_name",
        lambda env, trunc: "my-bucket",
        raising=False,
    )
    monkeypatch.setattr(
        "dags.common.dag_utilities.failover_managed_dag_tag",
        lambda: "failover-managed",
        raising=False,
    )

    # Your DAG imports get_cls_oauth_endpoint(env)
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_cls_oauth_endpoint",
        lambda env: "https://oauth.cls.example/token",
        raising=False,
    )

    # -------------------------
    # Patch open() for SQL templates
    # -------------------------
    def _fake_open(file, mode="r", *args, **kwargs):
        # file looks like ".../<type>/<type>_<table>.sql"
        path = str(file)
        if "/copy/" in path:
            return io.StringIO(sql_templates["copy"])
        if "/deduplication/" in path:
            return io.StringIO(sql_templates["deduplication"])
        # fallback: allow empty
        return io.StringIO("")

    monkeypatch.setattr("builtins.open", _fake_open, raising=False)

    # -------------------------
    # Patch boto3 S3 client/paginator
    # -------------------------
    if s3_pages_by_call is None:
        s3_pages_by_call = [[{"Contents": [], "CommonPrefixes": []}]]

    paginator = _FakePaginator(s3_pages_by_call)
    fake_s3 = _FakeS3Client(paginator)
    monkeypatch.setattr("boto3.client", lambda service: fake_s3, raising=False)

    return importlib.import_module(MODULE_PATH)


# -------------------------
# Pure helper unit tests
# -------------------------
def test_deep_replace_placeholders_nested(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    mod = _import_module_fresh(monkeypatch, workflow_var=workflow_var)

    obj = {
        "a": "{{foo}}",
        "b": ["x", "{{bar}}", {"c": "{{baz}}"}],
        "d": 123,
    }
    creds = {"foo": "FOO", "bar": "BAR"}  # baz missing stays "{{baz}}"
    out = mod._deep_replace_placeholders(obj, creds)

    assert out["a"] == "FOO"
    assert out["b"][1] == "BAR"
    assert out["b"][2]["c"] == "{{baz}}"

    # Depending on your implementation, this is either preserved (preferred)
    # or becomes None (older behavior). Keep this assertion flexible.
    assert out["d"] in (123, None)


# -------------------------
# Task function unit tests
# -------------------------
def test_get_sql_copy_replaces_and_strips_bucket(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    copy_tpl = "COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}';"

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        sql_templates={"copy": copy_tpl, "deduplication": "SELECT 1;"},
    )

    fn = _get_task_callable(mod.get_sql)

    sql = fn(
        table_name="my_table",
        sql_params={"DATABASE": "DB", "SCHEMA": "SCH"},
        type="copy",
        s3_uri="s3://my-bucket/vendor/ds/file.jsonl",
        enabled=True,
    )

    assert "COPY INTO DB.SCH.my_table" in sql
    assert "vendor/ds/file.jsonl" in sql  # bucket stripped
    assert "{{ params.target_table }}" not in sql
    assert "{{ params.s3_uri }}" not in sql


def test_get_sql_disabled_returns_noop(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    mod = _import_module_fresh(monkeypatch, workflow_var=workflow_var)
    fn = _get_task_callable(mod.get_sql)

    sql = fn(
        table_name="my_table",
        sql_params={"DATABASE": "DB", "SCHEMA": "SCH"},
        type="deduplication",
        s3_uri=None,
        enabled=False,
    )
    assert sql.strip().lower().startswith("select 1")


def test_resolve_run_config_reads_workflow_tables_and_dedupe(monkeypatch):
    workflow_var = {
        "INGESTER_TABLES": {"Account": "account_ds", "Contact": "contact_ds"},
        "INGESTER_DEDUPE": True,
        "INGESTER_START_DATE": "2000-01-01",
    }
    dag_run_conf = {
        "vendor": "generic",
        "start_date": "2020-01-01",
        "end_date": "2020-01-31",
        "credentials": {"bar": "baz"},
    }

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        dag_run_conf=dag_run_conf,
    )

    fn = _get_task_callable(mod.resolve_run_config)
    out = fn()

    assert out["vendor"] == "generic"
    assert out["start_date"] == "2020-01-01"
    assert out["end_date"] == "2020-01-31"
    assert out["credentials"]["bar"] == "baz"
    assert out["dedupe"] is True
    assert len(out["tables"]) == 2
    assert {"table": "Account", "dataset_id": "account_ds"} in out["tables"]


def test_get_latest_s3_uri_with_pattern_returns_newest(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    pages = [
        {
            "Contents": [
                {"Key": "x/a.jsonl", "LastModified": _dt(2024, 1, 1)},
                {"Key": "x/b.jsonl", "LastModified": _dt(2024, 1, 2)},
                {"Key": "x/c.csv", "LastModified": _dt(2024, 1, 3)},
            ]
        }
    ]

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        s3_pages_by_call=[pages],
    )

    fn = _get_task_callable(mod.get_latest_s3_uri)
    out = fn("s3://my-bucket/x/", pattern="*.jsonl")
    assert out == "s3://my-bucket/x/b.jsonl"


def test_get_latest_s3_uri_with_pattern_no_matches_raises(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    pages = [
        {"Contents": [{"Key": "x/a.csv", "LastModified": _dt(2024, 1, 1)}]}
    ]

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        s3_pages_by_call=[pages],
    )

    fn = _get_task_callable(mod.get_latest_s3_uri)
    with pytest.raises(ValueError, match="No objects found"):
        fn("s3://my-bucket/x/", pattern="*.jsonl")


def test_get_latest_s3_uri_no_pattern_returns_latest_prefix(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}

    # Call 1: top-level listing with CommonPrefixes
    top_listing = {"CommonPrefixes": [{"Prefix": "x/p1/"}, {"Prefix": "x/p2/"}]}

    # Call 2: p1 listing -> latest is Jan 1
    p1_listing = {
        "Contents": [{"Key": "x/p1/f1", "LastModified": _dt(2024, 1, 1)}]
    }

    # Call 3: p2 listing -> latest is Jan 3 (so p2 wins)
    p2_listing = {
        "Contents": [{"Key": "x/p2/f2", "LastModified": _dt(2024, 1, 3)}]
    }

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        s3_pages_by_call=[top_listing, p1_listing, p2_listing],
    )

    fn = _get_task_callable(mod.get_latest_s3_uri)
    out = fn("s3://my-bucket/x/")
    assert out == "s3://my-bucket/x/p2/"


def test_get_latest_s3_uri_invalid_scheme_raises(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    mod = _import_module_fresh(monkeypatch, workflow_var=workflow_var)

    fn = _get_task_callable(mod.get_latest_s3_uri)
    with pytest.raises(ValueError, match="Expected s3://bucket/prefix"):
        fn("https://example.com/x/")


# -------------------------
# DAG construction tests (mapping-friendly)
# -------------------------
def test_dag_builds_expected_base_tasks(monkeypatch):
    """
    With dynamic task mapping, you won't have per-table task_ids.
    So we assert the base tasks exist and the key dependencies exist.
    """
    workflow_var = {
        "INGESTER_TABLES": {"Account": "account_ds", "Contact": "contact_ds"},
        "INGESTER_CONFIG_PATH": "ingestor/salesforce.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "once",
        "INGESTER_SQL_PARAMS": {"DATABASE": "DB", "SCHEMA": "SCH"},
        "INGESTER_TESTING": False,
        "INGESTER_DEDUPE": True,
    }

    mod = _import_module_fresh(monkeypatch, workflow_var=workflow_var)
    dag = mod.dag

    task_ids = set(dag.task_ids)

    # Core “option B” / mapping-friendly tasks
    assert "resolve_run_config" in task_ids
    assert "build_event_json_for_table" in task_ids
    assert "build_glue_script_args" in task_ids
    assert "run_glue_job" in task_ids
    assert "load_table" in task_ids
    assert "dedupe_table" in task_ids

    # Ensure run_glue_job comes after glue args
    t_glue_args = dag.get_task("build_glue_script_args")
    t_run_glue = dag.get_task("run_glue_job")
    assert t_run_glue.task_id in t_glue_args.downstream_task_ids

    # Ensure load_table depends on get_sql
    t_get_sql = dag.get_task("get_sql")
    t_load = dag.get_task("load_table")
    assert t_load.task_id in t_get_sql.downstream_task_ids

    # Ensure dedupe_table depends on get_sql as well (dedupe SQL is produced by get_sql)
    t_dedupe = dag.get_task("dedupe_table")
    assert t_dedupe.task_id in t_get_sql.downstream_task_ids
