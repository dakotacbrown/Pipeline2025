# tests/unit/test_generic_ingester_dag.py
import importlib
import io
import json
import sys
from datetime import datetime, timezone

import pytest

MODULE_PATH = "dags.generic.generic_ingester"


def _dt(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


def _task_callable(obj):
    """
    Airflow TaskFlow @task returns a TaskDecorator in some versions (has .function).
    Operators typically expose .python_callable.
    """
    if hasattr(obj, "function"):
        return obj.function
    if hasattr(obj, "python_callable"):
        return obj.python_callable
    raise TypeError(f"Don't know how to get callable from {obj!r}")


class FakePaginator:
    """
    Router-based paginator so we can return different pages depending on kwargs.
    """

    def __init__(self, router):
        self._router = router

    def paginate(self, **kwargs):
        yield from self._router(kwargs)


class FakeS3Client:
    def __init__(self, paginator):
        self._paginator = paginator

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self._paginator


def _import_module_fresh(
    monkeypatch,
    tmp_path,
    *,
    workflow_var,
    copy_sql_template,
    dag_run_conf=None,
    s3_router=None,
):
    """
    Patch external deps BEFORE importing the DAG module so import-time DAG construction is safe.
    """
    # ensure Airflow-style env var isn't required by anything else
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", str(tmp_path))

    # Fresh import
    if MODULE_PATH in sys.modules:
        del sys.modules[MODULE_PATH]

    # -------------------------
    # Patch airflow Variable.get
    # -------------------------
    class FakeVariable:
        @staticmethod
        def get(key, default_var=None, deserialize_json=False):
            if key.startswith("INGESTER_WORKFLOW_"):
                return (
                    workflow_var
                    if deserialize_json
                    else json.dumps(workflow_var)
                )

            # used in DAG
            if key == "CISCOREDATASERVICES_GITHUB_PASSWORD":
                return "ghp_xxx"

            # exchange creds (support multiple key variants just in case)
            if key in (
                "CISCOREDATASERVICES_EXCHANGE_ID",
                "C1S_EXCHANGE_ID",
                "C1S_OAUTH_CLIENT_ID",
            ):
                return "ex_id"
            if key in (
                "CISCOREDATASERVICES_EXCHANGE_SECRET",
                "C1S_EXCHANGE_SECRET",
                "C1S_OAUTH_CLIENT_SECRET",
            ):
                return "ex_secret"

            return default_var

    monkeypatch.setattr("airflow.models.Variable", FakeVariable)

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

    def fake_get_current_context():
        return {"dag_run": type("DR", (), {"conf": dag_run_conf})()}

    monkeypatch.setattr(
        "airflow.operators.python.get_current_context", fake_get_current_context
    )

    # -------------------------
    # Patch dag_utilities getters
    # -------------------------
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_shairflow_environment", lambda: "dev"
    )
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_shairflow_region", lambda: "us-east-1"
    )
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_truncated_shairflow_region",
        lambda: "use1",
    )
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_bucket_name",
        lambda env, trunc: "my-bucket",
    )

    # IMPORTANT: c1s oauth endpoint helper
    monkeypatch.setattr(
        "dags.common.dag_utilities.get_c1s_oauth_endpoint",
        lambda env: "https://oauth.c1s.example/token",
    )

    # -------------------------
    # Patch open() for copy SQL templates
    # -------------------------
    def fake_open(*args, **kwargs):
        return io.StringIO(copy_sql_template)

    monkeypatch.setattr("builtins.open", fake_open)

    # -------------------------
    # Patch boto3 S3 client/paginator
    # -------------------------
    if s3_router is None:
        # default router: empty listings
        def s3_router(kwargs):
            return iter([{"Contents": [], "CommonPrefixes": []}])

    paginator = FakePaginator(s3_router)
    monkeypatch.setattr("boto3.client", lambda service: FakeS3Client(paginator))

    # Import module fresh
    return importlib.import_module(MODULE_PATH)


# -------------------------
# Pure helper tests
# -------------------------
def test_safe_task_id(monkeypatch, tmp_path):
    workflow_var = {"INGESTER_TABLES": {"My Table!!": "ds1"}}
    mod = _import_module_fresh(
        monkeypatch,
        tmp_path,
        workflow_var=workflow_var,
        copy_sql_template="",
    )
    assert mod._safe_task_id("My Table!!") == "my_table"
    assert mod._safe_task_id("__Already__Ok__") == "already__ok"
    assert mod._safe_task_id("   ") == ""


def test_deep_replace_placeholders_nested(monkeypatch, tmp_path):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    mod = _import_module_fresh(
        monkeypatch, tmp_path, workflow_var=workflow_var, copy_sql_template=""
    )

    obj = {"a": "{{foo}}", "b": ["x", "{{bar}}", {"c": "{{baz}}"}], "d": 123}
    creds = {"foo": "FOO", "bar": "BAR"}  # baz missing should remain
    out = mod._deep_replace_placeholders(obj, creds)

    assert out["a"] == "FOO"
    assert out["b"][1] == "BAR"
    assert out["b"][2]["c"] == "{{baz}}"
    assert out["d"] == 123


def test_get_copy_sql_replaces_and_strips_bucket(monkeypatch, tmp_path):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    copy_tpl = "COPY INTO {{ params.target_table }} FROM '{{ params.s3_url }}';"

    mod = _import_module_fresh(
        monkeypatch,
        tmp_path,
        workflow_var=workflow_var,
        copy_sql_template=copy_tpl,
    )

    fn = _task_callable(mod.get_copy_sql)
    sql = fn(
        table_name="my_table",
        sql_params={"DATABASE": "DB", "SCHEMA": "SCH"},
        s3_uri="s3://my-bucket/vendor/ds/file.jsonl",
    )
    assert "DB.SCH.my_table" in sql
    assert "vendor/ds/file.jsonl" in sql
    assert "s3://my-bucket" not in sql


# -------------------------
# get_latest_s3_uri tests
# -------------------------
def test_get_latest_s3_uri_with_pattern_returns_newest(monkeypatch, tmp_path):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}

    def router(kwargs):
        # single listing for the prefix
        return iter(
            [
                {
                    "Contents": [
                        {"Key": "x/a.jsonl", "LastModified": _dt(2024, 1, 1)},
                        {"Key": "x/b.jsonl", "LastModified": _dt(2024, 1, 2)},
                        {"Key": "x/c.csv", "LastModified": _dt(2024, 1, 3)},
                    ]
                }
            ]
        )

    mod = _import_module_fresh(
        monkeypatch,
        tmp_path,
        workflow_var=workflow_var,
        copy_sql_template="",
        s3_router=router,
    )

    fn = _task_callable(mod.get_latest_s3_uri)
    out = fn("s3://my-bucket/x/", pattern="*.jsonl")
    assert out == "s3://my-bucket/x/b.jsonl"


def test_get_latest_s3_uri_with_pattern_no_matches_raises(
    monkeypatch, tmp_path
):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}

    def router(kwargs):
        return iter(
            [
                {
                    "Contents": [
                        {"Key": "x/a.csv", "LastModified": _dt(2024, 1, 1)}
                    ]
                }
            ]
        )

    mod = _import_module_fresh(
        monkeypatch,
        tmp_path,
        workflow_var=workflow_var,
        copy_sql_template="",
        s3_router=router,
    )
    fn = _task_callable(mod.get_latest_s3_uri)

    with pytest.raises(ValueError, match="No objects found"):
        fn("s3://my-bucket/x/", pattern="*.jsonl")


def test_get_latest_s3_uri_no_pattern_returns_latest_prefix(
    monkeypatch, tmp_path
):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}

    def router(kwargs):
        # Top-level call has Delimiter="/"
        if kwargs.get("Delimiter") == "/":
            return iter(
                [{"CommonPrefixes": [{"Prefix": "x/p1/"}, {"Prefix": "x/p2/"}]}]
            )

        # Sub-listings by Prefix
        if kwargs.get("Prefix") == "x/p1/":
            return iter(
                [
                    {
                        "Contents": [
                            {"Key": "x/p1/f1", "LastModified": _dt(2024, 1, 1)}
                        ]
                    }
                ]
            )
        if kwargs.get("Prefix") == "x/p2/":
            return iter(
                [
                    {
                        "Contents": [
                            {"Key": "x/p2/f2", "LastModified": _dt(2024, 1, 3)}
                        ]
                    }
                ]
            )

        return iter([{"Contents": [], "CommonPrefixes": []}])

    mod = _import_module_fresh(
        monkeypatch,
        tmp_path,
        workflow_var=workflow_var,
        copy_sql_template="",
        s3_router=router,
    )
    fn = _task_callable(mod.get_latest_s3_uri)

    out = fn("s3://my-bucket/x/")
    assert out == "s3://my-bucket/x/p2/"


def test_get_latest_s3_uri_invalid_scheme_raises(monkeypatch, tmp_path):
    workflow_var = {"INGESTER_TABLES": {"t": "ds"}}
    mod = _import_module_fresh(
        monkeypatch, tmp_path, workflow_var=workflow_var, copy_sql_template=""
    )
    fn = _task_callable(mod.get_latest_s3_uri)

    with pytest.raises(ValueError, match="Expected s3://bucket/prefix"):
        fn("https://example.com/x/")


# -------------------------
# DAG construction tests
# -------------------------
def test_dag_builds_expected_tasks_and_dependencies(monkeypatch, tmp_path):
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

    # Provide just enough so the module import + DAG build references are valid
    def router(kwargs):
        # For any call, return an empty-ish listing. The tasks won't execute in this test.
        return iter(
            [
                {
                    "Contents": [
                        {
                            "Key": "code/ETL/debi-etl-framework-glue1.zip",
                            "LastModified": _dt(2024, 1, 1),
                        }
                    ]
                }
            ]
        )

    mod = _import_module_fresh(
        monkeypatch,
        tmp_path,
        workflow_var=workflow_var,
        copy_sql_template="COPY INTO {{ params.target_table }} FROM '{{ params.s3_url }}';",
        s3_router=router,
    )

    dag = mod.dag
    task_ids = set(dag.task_ids)

    assert "latest_framework_zip" in task_ids

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


def test_latest_jsonl_prefix_changes_in_testing_mode(monkeypatch, tmp_path):
    workflow_var = {
        "INGESTER_TABLES": {"Account": "account_ds"},
        "INGESTER_TESTING": True,
    }

    mod = _import_module_fresh(
        monkeypatch, tmp_path, workflow_var=workflow_var, copy_sql_template=""
    )
    dag = mod.dag

    safe = mod._safe_task_id("Account")
    t = dag.get_task(f"latest_jsonl_{safe}")

    # TaskFlow @task becomes an operator with op_kwargs
    assert t.op_kwargs["s3_prefix"] == "s3://my-bucket/test/generic/account_ds"


def test_event_json_includes_exchange_and_data_extras_with_placeholder_replacement(
    monkeypatch, tmp_path
):
    workflow_var = {
        "INGESTER_TABLES": {"Account": "account_ds"},
        "INGESTER_EXCHANGE": True,
        "INGESTER_DATA_EXTRAS": {
            "extra_foo": "{{bar}}",
            "nested": {"k": "{{bar}}"},
        },
    }

    dag_run_conf = {"vendor": "generic", "credentials": {"bar": "baz"}}

    mod = _import_module_fresh(
        monkeypatch,
        tmp_path,
        workflow_var=workflow_var,
        copy_sql_template="",
        dag_run_conf=dag_run_conf,
    )

    dag = mod.dag
    safe = mod._safe_task_id("Account")
    build_task = dag.get_task(f"build_event_{safe}")

    payload = json.loads(
        build_task.python_callable(table="Account", dataset_id="account_ds")
    )

    assert payload["table"] == "Account"
    assert payload["vendor"] == "generic"
    assert payload["dataset_id"] == "account_ds"

    # c1s oauth URL present + backward compat key
    assert payload["c1s_oauth_url"] == "https://oauth.c1s.example/token"
    assert payload["cl_oauth_url"] == "https://oauth.c1s.example/token"

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
