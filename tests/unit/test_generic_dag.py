# tests/unit/test_generic_ingester_dag.py
import importlib
import io
import json
import sys
from datetime import datetime, timezone

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
            # no more pages configured
            return iter([])

        pages = self._pages_by_call[self._call_idx]
        self._call_idx += 1

        if pages is None:
            return iter([])

        if isinstance(pages, dict):
            return iter([pages])

        # assume list of dict pages
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
      - operator instances (PythonDecoratedOperator) -> has .python_callable
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
    workflow_var,
    copy_sql_template,
    dag_run_conf=None,
    s3_pages_by_call=None,
):
    """
    Patch all external deps BEFORE importing the DAG module so module-level DAG creation works.
    """
    # Ensure fresh import
    if MODULE_PATH in sys.modules:
        del sys.modules[MODULE_PATH]

    # -------------------------
    # Patch Airflow Variable.get
    # -------------------------
    def _fake_variable_get(key, default_var=None, deserialize_json=False):
        # Matches how your DAG reads:
        #   Variable.get(f"INGESTER_WORKFLOW_{vendor.upper()}", default_var={}, deserialize_json=True)
        if key.startswith("INGESTER_WORKFLOW_"):
            return (
                workflow_var if deserialize_json else json.dumps(workflow_var)
            )

        # Exchange + github token reads
        if key == "C1SCOREDATASERVICES_GITHUB_PASSWORD":
            return "ghp_xxx"
        if key == "C1SCOREDATASERVICES_EXCHANGE_ID":
            return "ex_id"
        if key == "C1SCOREDATASERVICES_EXCHANGE_SECRET":
            return "ex_secret"

        return default_var

    # Patch the class method directly (prevents sqlite metastore access)
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
        # needs context["dag_run"].conf and conf.get(...)
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
    # IMPORTANT: your DAG now calls get_c1s_oauth_endpoint(env)
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_c1s_oauth_endpoint",
        lambda env: "https://oauth.c1s.example/token",
        raising=False,
    )

    # -------------------------
    # Patch open() for copy SQL templates
    # -------------------------
    def _fake_open(*args, **kwargs):
        return io.StringIO(copy_sql_template)

    monkeypatch.setattr("builtins.open", _fake_open, raising=False)

    # -------------------------
    # Patch boto3 S3 client/paginator
    # -------------------------
    if s3_pages_by_call is None:
        # default: enough for module import (zip + jsonl lookups won’t execute anyway)
        s3_pages_by_call = [[{"Contents": [], "CommonPrefixes": []}]]

    paginator = _FakePaginator(s3_pages_by_call)
    fake_s3 = _FakeS3Client(paginator)
    monkeypatch.setattr("boto3.client", lambda service: fake_s3, raising=False)

    # Import fresh
    return importlib.import_module(MODULE_PATH)


# -------------------------
# Pure helper unit tests
# -------------------------
def test_safe_task_id(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"My Table!!": "ds1"}}
    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="",
        s3_pages_by_call=[[{"Contents": [], "CommonPrefixes": []}]],
    )

    assert mod._safe_task_id("My Table!!") == "my_table"
    assert mod._safe_task_id("__Already__OK__") == "already_ok"
    assert mod._safe_task_id("  ") == ""


def test_deep_replace_placeholders_nested_current_behavior(monkeypatch):
    """
    NOTE: This asserts CURRENT behavior of your implementation:
    - dict/list/str handled
    - other scalar types return None (because there's no final `return obj`)
    """
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="",
        s3_pages_by_call=[[{"Contents": [], "CommonPrefixes": []}]],
    )

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
    assert out["d"] is None  # current implementation returns None for ints


# -------------------------
# Task function unit tests
# -------------------------
def test_get_copy_sql_replaces_and_strips_bucket(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    copy_tpl = "COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}';"

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template=copy_tpl,
        s3_pages_by_call=[[{"Contents": [], "CommonPrefixes": []}]],
    )

    fn = _get_task_callable(mod.get_copy_sql)

    sql = fn(
        table_name="my_table",
        sql_params={"DATABASE": "DB", "SCHEMA": "SCH"},
        s3_uri="s3://my-bucket/vendor/ds/file.jsonl",
    )

    assert "COPY INTO DB.SCH.my_table" in sql
    assert "vendor/ds/file.jsonl" in sql  # bucket stripped
    assert "{{ params.target_table }}" not in sql
    assert "{{ params.s3_uri }}" not in sql


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
        copy_sql_template="",
        s3_pages_by_call=[pages],
    )

    fn = _get_task_callable(mod.get_latest_s3_uri)
    out = fn("s3://my-bucket/x/", pattern="*.jsonl")
    assert out == "s3://my-bucket/x/b.jsonl"


def test_get_latest_s3_uri_with_pattern_no_matches_raises(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    pages = [
        {"Contents": [{"Key": "x/a.csv", "LastModified": _dt(2024, 1, 1)}]},
    ]

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="",
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
        copy_sql_template="",
        s3_pages_by_call=[top_listing, p1_listing, p2_listing],
    )

    fn = _get_task_callable(mod.get_latest_s3_uri)
    out = fn("s3://my-bucket/x/")
    assert out == "s3://my-bucket/x/p2/"


def test_get_latest_s3_uri_invalid_scheme_raises(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="",
        s3_pages_by_call=[[{"Contents": [], "CommonPrefixes": []}]],
    )

    fn = _get_task_callable(mod.get_latest_s3_uri)
    with pytest.raises(ValueError, match="Expected s3://bucket/prefix"):
        fn("https://example.com/x/")


# -------------------------
# DAG construction tests
# -------------------------
def test_dag_builds_expected_tasks_and_dependencies(monkeypatch):
    workflow_var = {
        "INGESTER_TABLES": {"Account": "account_ds", "Contact": "contact_ds"},
        "INGESTER_CONFIG_PATH": "ingestor/salesforce.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "once",
        "INGESTER_SQL_PARAMS": {"DATABASE": "DB", "SCHEMA": "SCH"},
        "INGESTER_TESTING": False,
    }

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}';",
        s3_pages_by_call=[[{"Contents": [], "CommonPrefixes": []}]],
    )

    dag = mod.dag
    task_ids = set(dag.task_ids)

    # Global task
    assert "latest_framework_zip" in task_ids

    # Per-table tasks and dependency chain
    for raw in ["Account", "Contact"]:
        safe = mod._safe_task_id(raw)
        assert f"build_event_{safe}" in task_ids
        assert f"run_glue_job_{safe}" in task_ids
        assert f"latest_jsonl_{safe}" in task_ids
        assert f"copy_sql_{safe}" in task_ids
        assert f"load_table_{safe}" in task_ids

        t_latest_zip = dag.get_task("latest_framework_zip")
        t_build = dag.get_task(f"build_event_{safe}")
        t_glue = dag.get_task(f"run_glue_job_{safe}")
        t_latest_jsonl = dag.get_task(f"latest_jsonl_{safe}")
        t_copy = dag.get_task(f"copy_sql_{safe}")
        t_load = dag.get_task(f"load_table_{safe}")

        assert t_build.task_id in t_latest_zip.downstream_task_ids
        assert t_glue.task_id in t_build.downstream_task_ids
        assert t_latest_jsonl.task_id in t_glue.downstream_task_ids
        assert t_copy.task_id in t_latest_jsonl.downstream_task_ids
        assert t_load.task_id in t_copy.downstream_task_ids


def test_latest_jsonl_prefix_changes_in_testing_mode(monkeypatch):
    workflow_var = {
        "INGESTER_TABLES": {"Account": "account_ds"},
        "INGESTER_SQL_PARAMS": {"DATABASE": "DB", "SCHEMA": "SCH"},
        "INGESTER_TESTING": True,
    }

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}';",
        s3_pages_by_call=[[{"Contents": [], "CommonPrefixes": []}]],
    )

    dag = mod.dag
    safe = mod._safe_task_id("Account")
    t = dag.get_task(f"latest_jsonl_{safe}")

    # TaskFlow task becomes a PythonDecoratedOperator with op_kwargs
    assert t.op_kwargs["s3_prefix"] == "s3://my-bucket/test/generic/account_ds"


def test_event_json_includes_exchange_and_data_extras_placeholder_replacement(
    monkeypatch,
):
    workflow_var = {
        "INGESTER_TABLES": {"Account": "account_ds"},
        "INGESTER_SQL_PARAMS": {"DATABASE": "DB", "SCHEMA": "SCH"},
        "INGESTER_EXCHANGE": True,
        "INGESTER_DATA_EXTRAS": {
            "extra_foo": "{{bar}}",
            "nested": {"k": "{{bar}}"},
        },
    }

    dag_run_conf = {
        "vendor": "generic",
        "credentials": {"bar": "baz"},
    }

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}';",
        dag_run_conf=dag_run_conf,
        s3_pages_by_call=[[{"Contents": [], "CommonPrefixes": []}]],
    )

    dag = mod.dag
    safe = mod._safe_task_id("Account")
    build_task = dag.get_task(f"build_event_{safe}")

    payload = json.loads(
        build_task.python_callable(table="Account", dataset_id="account_ds")
    )

    # Base
    assert payload["table"] == "Account"
    assert payload["vendor"] == "generic"
    assert payload["dataset_id"] == "account_ds"

    # c1s oauth URL exists (from get_c1s_oauth_endpoint monkeypatch)
    assert payload["c1_oauth_url"] == "https://oauth.c1s.example/token"

    # Exchange merged
    assert (
        payload["exchange_headers"]["Content-Type"]
        == "application/x-www-form-urlencoded"
    )
    assert payload["exchange_data"]["client_id"] == "ex_id"
    assert payload["exchange_data"]["client_secret"] == "ex_secret"
    assert payload["exchange_data"]["grant_type"] == "client_credentials"

    # Data extras placeholder replaced
    assert payload["extra_foo"] == "baz"
    assert payload["nested"]["k"] == "baz"
