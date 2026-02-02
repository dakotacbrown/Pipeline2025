import importlib
import io
import json
from datetime import datetime, timezone

import pytest


MODULE_PATH = "dags.generic.generic_ingester"


def _dt(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        # return an iterator of dict pages
        for p in self._pages:
            yield p


class _FakeS3Client:
    def __init__(self, paginator):
        self._paginator = paginator

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self._paginator


def _import_module_fresh(monkeypatch, *, workflow_var, copy_sql_template, dag_run_conf=None, s3_pages=None):
    """
    Patch all external deps BEFORE importing the DAG module so module-level DAG creation works.
    """
    import sys

    # Ensure fresh import
    if MODULE_PATH in sys.modules:
        del sys.modules[MODULE_PATH]

    # -------------------------
    # Patch airflow Variable.get
    # -------------------------
    class _FakeVariable:
        @staticmethod
        def get(key, default_var=None, deserialize_json=False):
            if key.startswith("INGESTER_WORKFLOW_"):
                return workflow_var if deserialize_json else json.dumps(workflow_var)
            # used by exchange creds + github token
            if key == "CISCOREDATASERVICES_GITHUB_PASSWORD":
                return "ghp_xxx"
            if key == "CISCOREDATASERVICES_EXCHANGE_ID":
                return "ex_id"
            if key == "CISCOREDATASERVICES_EXCHANGE_SECRET":
                return "ex_secret"
            return default_var

    monkeypatch.setattr("airflow.models.Variable", _FakeVariable)

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
        return {"dag_run": type("DR", (), {"conf": dag_run_conf})()}

    monkeypatch.setattr("airflow.operators.python.get_current_context", _fake_get_current_context)

    # -------------------------
    # Patch your dag_utilities getters
    # -------------------------
    monkeypatch.setattr("dags.common.dag_utilities.get_shairflow_environment", lambda: "dev")
    monkeypatch.setattr("dags.common.dag_utilities.get_shairflow_region", lambda: "us-east-1")
    monkeypatch.setattr("dags.common.dag_utilities.get_truncated_shairflow_region", lambda: "use1")
    monkeypatch.setattr("dags.common.dag_utilities.get_bucket_name", lambda env, trunc: "my-bucket")

    # <-- THE IMPORTANT CHANGE: c1s -->
    monkeypatch.setattr("dags.common.dag_utilities.get_c1s_oauth_endpoint", lambda env: "https://oauth.c1s.example/token")

    # -------------------------
    # Patch open() for copy SQL templates
    # -------------------------
    def _fake_open(*args, **kwargs):
        return io.StringIO(copy_sql_template)

    monkeypatch.setattr("builtins.open", _fake_open)

    # -------------------------
    # Patch boto3 S3 client/paginator
    # -------------------------
    if s3_pages is None:
        # default: enough to satisfy the DAG import (zip + jsonl lookups)
        s3_pages = [{"Contents": [], "CommonPrefixes": []}]

    paginator = _FakePaginator(s3_pages)
    fake_s3 = _FakeS3Client(paginator)

    monkeypatch.setattr("boto3.client", lambda service: fake_s3)

    # Import module fresh
    mod = importlib.import_module(MODULE_PATH)
    return mod


# -------------------------
# Pure helper unit tests
# -------------------------
def test_safe_task_id(monkeypatch):
    # Import with minimal stubs
    workflow_var = {"INGESTER_TABLES": {"My Table!!": "ds1"}}
    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="",
        s3_pages=[{"Contents": [], "CommonPrefixes": []}],
    )
    assert mod._safe_task_id("My Table!!") == "my_table"
    assert mod._safe_task_id("__Already__Ok__") == "already__ok"
    assert mod._safe_task_id("   ") == ""


def test_deep_replace_placeholders_nested(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="",
        s3_pages=[{"Contents": [], "CommonPrefixes": []}],
    )

    obj = {
        "a": "{{foo}}",
        "b": ["x", "{{bar}}", {"c": "{{baz}}"}],
        "d": 123,
    }
    creds = {"foo": "FOO", "bar": "BAR"}  # baz missing should remain "{{baz}}"
    out = mod._deep_replace_placeholders(obj, creds)

    assert out["a"] == "FOO"
    assert out["b"][1] == "BAR"
    assert out["b"][2]["c"] == "{{baz}}"
    assert out["d"] == 123


def test_get_copy_sql_replaces_and_strips_bucket(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    copy_tpl = "COPY INTO {{ params.target_table }} FROM '{{ params.s3_url }}';"

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template=copy_tpl,
        s3_pages=[{"Contents": [], "CommonPrefixes": []}],
    )

    # TaskFlow decorated; call underlying python
    fn = mod.get_copy_sql.python_callable
    sql = fn(
        table_name="my_table",
        sql_params={"DATABASE": "DB", "SCHEMA": "SCH"},
        s3_uri="s3://my-bucket/vendor/ds/file.jsonl",
    )
    assert "DB.SCH.my_table" in sql
    assert "vendor/ds/file.jsonl" in sql  # bucket stripped
    assert "s3://my-bucket" not in sql


# -------------------------
# get_latest_s3_uri tests
# -------------------------
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
        s3_pages=pages,
    )

    fn = mod.get_latest_s3_uri.python_callable
    out = fn("s3://my-bucket/x/", pattern="*.jsonl")
    assert out == "s3://my-bucket/x/b.jsonl"


def test_get_latest_s3_uri_with_pattern_no_matches_raises(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    pages = [{"Contents": [{"Key": "x/a.csv", "LastModified": _dt(2024, 1, 1)}]}]

    mod = _import_module_fresh(monkeypatch, workflow_var=workflow_var, copy_sql_template="", s3_pages=pages)
    fn = mod.get_latest_s3_uri.python_callable

    with pytest.raises(ValueError, match="No objects found"):
        fn("s3://my-bucket/x/", pattern="*.jsonl")


def test_get_latest_s3_uri_no_pattern_returns_latest_prefix(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}

    # First paginate call uses Delimiter="/" -> CommonPrefixes
    # Then for each prefix, it paginates again with Prefix=path to find latest object timestamp
    pages = [
        {"CommonPrefixes": [{"Prefix": "x/p1/"}, {"Prefix": "x/p2/"}]},  # top-level listing
        {"Contents": [{"Key": "x/p1/f1", "LastModified": _dt(2024, 1, 1)}]},  # p1 listing
        {"Contents": [{"Key": "x/p2/f2", "LastModified": _dt(2024, 1, 3)}]},  # p2 listing
    ]

    mod = _import_module_fresh(monkeypatch, workflow_var=workflow_var, copy_sql_template="", s3_pages=pages)
    fn = mod.get_latest_s3_uri.python_callable
    out = fn("s3://my-bucket/x/")
    assert out == "s3://my-bucket/x/p2/"


def test_get_latest_s3_uri_invalid_scheme_raises(monkeypatch):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    mod = _import_module_fresh(monkeypatch, workflow_var=workflow_var, copy_sql_template="", s3_pages=[{}])
    fn = mod.get_latest_s3_uri.python_callable

    with pytest.raises(ValueError, match="Expected s3://bucket/prefix"):
        fn("https://example.com/x/")


# -------------------------
# DAG construction tests (big coverage win)
# -------------------------
def test_dag_builds_expected_tasks_and_dependencies(monkeypatch):
    workflow_var = {
        "INGESTER_TABLES": {"Account": "account_ds", "Contact": "contact_ds"},
        "INGESTER_CONFIG_PATH": "ingester/salesforce.yml",
        "INGESTER_CONFIG_REPO_NAME": "config_management",
        "INGESTER_GLUE_JOB_NAME": "etl-job",
        "INGESTER_GLUE_CONN_NAME": "etl-net-conn",
        "INGESTER_RUN_MODE": "once",
        "INGESTER_SQL_PARAMS": {"DATABASE": "DB", "SCHEMA": "SCH"},
        "INGESTER_TESTING": False,
    }

    # Make S3 listing return something for zip and jsonl lookups (task callables aren’t executed in this test),
    # but module import creates DAG and tasks referencing those callables.
    pages = [{"Contents": [{"Key": "code/ETL/debi-etl-framework-glue1.zip", "LastModified": _dt(2024, 1, 1)}]}]

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="COPY INTO {{ params.target_table }} FROM '{{ params.s3_url }}';",
        s3_pages=pages,
    )

    dag = mod.dag
    task_ids = set(dag.task_ids)

    # Global task
    assert "latest_framework_zip" in task_ids

    # Per-table tasks
    for raw in ["Account", "Contact"]:
        safe = mod._safe_task_id(raw)
        assert f"build_event_{safe}" in task_ids
        assert f"run_glue_job_{safe}" in task_ids
        assert f"latest_jsonl_{safe}" in task_ids
        assert f"copy_sql_{safe}" in task_ids
        assert f"load_table_{safe}" in task_ids

        # Dependency chain assertions
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
        "INGESTER_TESTING": True,  # <-- key behavior
    }

    mod = _import_module_fresh(monkeypatch, workflow_var=workflow_var, copy_sql_template="", s3_pages=[{}])
    dag = mod.dag

    safe = mod._safe_task_id("Account")
    t = dag.get_task(f"latest_jsonl_{safe}")

    # TaskFlow @task becomes an operator with op_kwargs
    s3_prefix = t.op_kwargs["s3_prefix"]
    assert s3_prefix == "s3://my-bucket/test/generic/account_ds"


def test_event_json_includes_exchange_and_data_extras_with_placeholder_replacement(monkeypatch):
    workflow_var = {
        "INGESTER_TABLES": {"Account": "account_ds"},
        "INGESTER_EXCHANGE": True,
        "INGESTER_DATA_EXTRAS": {"extra_foo": "{{bar}}", "nested": {"k": "{{bar}}"}},
    }

    dag_run_conf = {
        "vendor": "generic",
        "credentials": {"bar": "baz"},
    }

    mod = _import_module_fresh(
        monkeypatch,
        workflow_var=workflow_var,
        copy_sql_template="",
        dag_run_conf=dag_run_conf,
        s3_pages=[{}],
    )

    dag = mod.dag
    safe = mod._safe_task_id("Account")
    build_task = dag.get_task(f"build_event_{safe}")

    # Underlying callable returns JSON string
    payload = json.loads(build_task.python_callable(table="Account", dataset_id="account_ds"))

    # Base
    assert payload["table"] == "Account"
    assert payload["vendor"] == "generic"
    assert payload["dataset_id"] == "account_ds"

    # c1s oauth URL exists (comes from get_c1s_oauth_endpoint monkeypatch)
    assert payload["cl_oauth_url"] == "https://oauth.c1s.example/token"

    # Exchange merged
    assert payload["exchange_headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert payload["exchange_data"]["client_id"] == "ex_id"
    assert payload["exchange_data"]["client_secret"] == "ex_secret"
    assert payload["exchange_data"]["grant_type"] == "client_credentials"

    # Data extras placeholder replaced
    assert payload["extra_foo"] == "baz"
    assert payload["nested"]["k"] == "baz"
