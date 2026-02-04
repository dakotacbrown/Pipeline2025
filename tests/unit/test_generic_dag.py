# tests/unit/test_generic_ingester_dag.py
from __future__ import annotations

import importlib.util
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

# ---------------------------------------------------------------------
# Configure these for your repo
# ---------------------------------------------------------------------
DAG_FILE_RELATIVE_PATH = Path("dags/generic/generic_ingester.py")
DAG_MODULE_NAME = "generic_ingester_under_test"


def _project_root() -> Path:
    # tests live in: <repo>/tests/unit/test_*.py
    return Path(__file__).resolve().parents[2]


def _install_stub_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Minimal stub modules so importing the DAG file doesn't require
    your full dags.common package during unit tests.
    """
    dags_mod = types.ModuleType("dags")
    common_mod = types.ModuleType("dags.common")

    dag_utils_mod = types.ModuleType("dags.common.dag_utilities")
    slack_mod = types.ModuleType("dags.common.slack")
    udf_mod = types.ModuleType("dags.common.user_defined_filters")

    def failover_managed_dag_tag() -> str:
        return "failover-managed"

    def get_bucket_name(env: str, truncated_region: str) -> str:
        return f"bucket-{env}-{truncated_region}"

    def get_c1s_oauth_endpoint(env: str) -> str:
        return f"https://oauth.example/{env}"

    def get_shairflow_environment() -> str:
        return "DEV"

    def get_shairflow_region() -> str:
        return "us-east-1"

    def get_truncated_shairflow_region() -> str:
        return "use1"

    dag_utils_mod.failover_managed_dag_tag = failover_managed_dag_tag
    dag_utils_mod.get_bucket_name = get_bucket_name
    dag_utils_mod.get_c1s_oauth_endpoint = get_c1s_oauth_endpoint
    dag_utils_mod.get_shairflow_environment = get_shairflow_environment
    dag_utils_mod.get_shairflow_region = get_shairflow_region
    dag_utils_mod.get_truncated_shairflow_region = (
        get_truncated_shairflow_region
    )

    def task_fail_slack_alert(*args: Any, **kwargs: Any) -> None:
        return None

    slack_mod.task_fail_slack_alert = task_fail_slack_alert

    def ts_nodash_to_YYYYMMDDHHmmss(value: str) -> str:
        return value

    udf_mod.ts_nodash_to_YYYYMMDDHHmmss = ts_nodash_to_YYYYMMDDHHmmss

    monkeypatch.setitem(sys.modules, "dags", dags_mod)
    monkeypatch.setitem(sys.modules, "dags.common", common_mod)
    monkeypatch.setitem(sys.modules, "dags.common.dag_utilities", dag_utils_mod)
    monkeypatch.setitem(sys.modules, "dags.common.slack", slack_mod)
    monkeypatch.setitem(
        sys.modules, "dags.common.user_defined_filters", udf_mod
    )


def _load_module_from_path(
    module_name: str, file_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _install_stub_modules(monkeypatch)

    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module spec from {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def dag_module(monkeypatch: pytest.MonkeyPatch):
    file_path = _project_root() / DAG_FILE_RELATIVE_PATH
    mod = _load_module_from_path(DAG_MODULE_NAME, file_path, monkeypatch)
    try:
        yield mod
    finally:
        sys.modules.pop(DAG_MODULE_NAME, None)


# ---------------------------------------------------------------------
# Helper: execute TaskFlow @task underlying callable (Airflow 2.10.5)
# ---------------------------------------------------------------------
def call_task(task_obj, *args, **kwargs):
    """
    Pass the TaskFlow task object itself (e.g., dag_module.resolve_run_config),
    not dag_module.resolve_run_config(...), which would return an XComArg.
    """
    try:
        from airflow.models.xcom_arg import XComArg  # type: ignore
    except Exception:  # pragma: no cover
        XComArg = ()  # type: ignore

    if isinstance(task_obj, XComArg):
        raise TypeError(
            "call_task() received an XComArg. "
            "Pass the task function itself (e.g. dag_module.my_task), not dag_module.my_task(...)."
        )

    fn = getattr(task_obj, "function", None)
    if callable(fn):
        return fn(*args, **kwargs)

    wrapped = getattr(task_obj, "__wrapped__", None)
    if callable(wrapped):
        return wrapped(*args, **kwargs)

    raise TypeError(f"Object {task_obj!r} does not look like a TaskFlow task")


# ---------------------------------------------------------------------
# Unit tests: _deep_replace_placeholders
# ---------------------------------------------------------------------
def test_deep_replace_placeholders_leaves_missing_and_none_intact(dag_module):
    fn = dag_module._deep_replace_placeholders

    obj = {
        "a": "{{TOKEN}}",
        "b": ["x", "{{MISSING}}", {"c": "{{NONEVAL}}"}],
        "d": 123,
    }
    creds = {"TOKEN": "abc123", "NONEVAL": None}

    out = fn(obj, creds)
    assert out["a"] == "abc123"
    assert out["b"][1] == "{{MISSING}}"
    assert out["b"][2]["c"] == "{{NONEVAL}}"
    assert out["d"] == 123


# ---------------------------------------------------------------------
# Unit tests: reconcile_table_specs
# ---------------------------------------------------------------------
def test_reconcile_table_specs_dedupes_preserves_order(dag_module):
    specs = [
        {"table": "a", "dataset_id": "1"},
        {"table": "b", "dataset_id": "2"},
        {"table": "a", "dataset_id": "1"},  # dup
    ]
    out = call_task(dag_module.reconcile_table_specs, specs)
    assert out == [
        {"table": "a", "dataset_id": "1"},
        {"table": "b", "dataset_id": "2"},
    ]


def test_reconcile_table_specs_requires_table_and_dataset(dag_module):
    with pytest.raises(ValueError):
        call_task(dag_module.reconcile_table_specs, [{"table": "a"}])


# ---------------------------------------------------------------------
# Unit tests: resolve_run_config (mock context + Variable.get)
# ---------------------------------------------------------------------
class _FakeDagRun:
    def __init__(self, conf: Optional[dict]):
        self.conf = conf


def test_resolve_run_config_requires_vendor(monkeypatch, dag_module):
    def fake_ctx():
        return {"dag_run": _FakeDagRun(conf={})}

    monkeypatch.setattr(dag_module, "get_current_context", fake_ctx)

    with pytest.raises(
        ValueError, match=r"dag_run\.conf\['vendor'\] is required"
    ):
        call_task(dag_module.resolve_run_config)


def test_resolve_run_config_exchange_enabled_true(monkeypatch, dag_module):
    def fake_ctx():
        return {
            "dag_run": _FakeDagRun(
                conf={
                    "vendor": "SalesForce",
                    "credentials": {"TOKEN": "t"},
                    "start_date": "2020-01-01",
                    "end_date": "2020-01-31",
                }
            )
        }

    monkeypatch.setattr(dag_module, "get_current_context", fake_ctx)

    def fake_variable_get(key: str, default_var=None, deserialize_json=False):
        if key == "INGESTER_WORKFLOW_SALESFORCE":
            return {
                "INGESTER_TABLES": {"accounts": "ds1", "contacts": "ds2"},
                "INGESTER_TESTING": True,
                "INGESTER_DEDUPE": False,
                "INGESTER_PYTHON_MODULE": "x==1.2.3",
                "INGESTER_GLUE_JOB_NAME": "jobname",
                "INGESTER_GLUE_CONN_NAME": "connname",
                "INGESTER_RUN_MODE": "once",
                "INGESTER_CONFIG_PATH": "path/to/config.yml",
                "INGESTER_CONFIG_REPO_NAME": "repo",
                "INGESTER_SQL_PARAMS": {"DATABASE": "DB", "SCHEMA": "SC"},
                "INGESTER_ENV_VARS": {"A": "B"},
                "INGESTER_DATA_EXTRAS": {"hello": "{{TOKEN}}"},
                "INGESTER_EXCHANGE": True,
            }
        return default_var

    monkeypatch.setattr(dag_module.Variable, "get", fake_variable_get)

    cfg = call_task(dag_module.resolve_run_config)

    assert cfg["vendor"] == "salesforce"
    assert cfg["tables"] == [
        {"table": "accounts", "dataset_id": "ds1"},
        {"table": "contacts", "dataset_id": "ds2"},
    ]
    assert cfg["credentials"] == {"TOKEN": "t"}
    assert cfg["exchange_enabled"] is True


def test_resolve_run_config_exchange_enabled_default_false(
    monkeypatch, dag_module
):
    def fake_ctx():
        return {"dag_run": _FakeDagRun(conf={"vendor": "x"})}

    monkeypatch.setattr(dag_module, "get_current_context", fake_ctx)

    def fake_variable_get(key: str, default_var=None, deserialize_json=False):
        if key == "INGESTER_WORKFLOW_X":
            return {"INGESTER_TABLES": {"t": "ds"}}  # no INGESTER_EXCHANGE key
        return default_var

    monkeypatch.setattr(dag_module.Variable, "get", fake_variable_get)

    cfg = call_task(dag_module.resolve_run_config)
    assert cfg["exchange_enabled"] is False


# ---------------------------------------------------------------------
# Unit tests: get_latest_s3_uri (mock boto3 paginator)
# ---------------------------------------------------------------------
class _FakePaginator:
    def __init__(self, pages: List[dict]):
        self._pages = pages

    def paginate(self, **kwargs):
        for p in self._pages:
            yield p


class _FakeS3Client:
    def __init__(self, paginator):
        self._paginator = paginator

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return self._paginator


def test_get_latest_s3_uri_invalid_scheme_raises(dag_module):
    with pytest.raises(ValueError, match=r"Expected s3://bucket/prefix"):
        call_task(dag_module.get_latest_s3_uri, "https://example.com/x")


def test_get_latest_s3_uri_pattern_branch_newest_match(monkeypatch, dag_module):
    dt1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    dt2 = datetime(2024, 1, 2, tzinfo=timezone.utc)

    pages = [
        {
            "Contents": [
                {"Key": "code/ETL/a.txt", "LastModified": dt1},
                {
                    "Key": "code/ETL/debi-etl-framework-glue-1.zip",
                    "LastModified": dt1,
                },
                {
                    "Key": "code/ETL/debi-etl-framework-glue-2.zip",
                    "LastModified": dt2,
                },
            ]
        }
    ]
    fake_s3 = _FakeS3Client(_FakePaginator(pages))
    monkeypatch.setattr(dag_module.boto3, "client", lambda name: fake_s3)

    out = call_task(
        dag_module.get_latest_s3_uri,
        "s3://my-bucket/code/ETL",
        pattern="debi-etl-framework-glue*.zip",
    )
    assert out == "s3://my-bucket/code/ETL/debi-etl-framework-glue-2.zip"


def test_get_latest_s3_uri_prefix_branch_latest_common_prefix(
    monkeypatch, dag_module
):
    dt_old = datetime(2024, 1, 1, tzinfo=timezone.utc)
    dt_new = datetime(2024, 1, 5, tzinfo=timezone.utc)

    pages_for_delimiter = [
        {
            "CommonPrefixes": [
                {"Prefix": "vendor/ds1/"},
                {"Prefix": "vendor/ds2/"},
            ]
        }
    ]
    pages_for_ds1 = [
        {"Contents": [{"Key": "vendor/ds1/file.json", "LastModified": dt_old}]}
    ]
    pages_for_ds2 = [
        {"Contents": [{"Key": "vendor/ds2/file.json", "LastModified": dt_new}]}
    ]

    class _SmartPaginator:
        def paginate(self, **kwargs):
            if kwargs.get("Delimiter") == "/":
                yield from pages_for_delimiter
                return
            if kwargs.get("Prefix") == "vendor/ds1/":
                yield from pages_for_ds1
                return
            if kwargs.get("Prefix") == "vendor/ds2/":
                yield from pages_for_ds2
                return
            yield {"Contents": []}

    fake_s3 = _FakeS3Client(_SmartPaginator())
    monkeypatch.setattr(dag_module.boto3, "client", lambda name: fake_s3)

    out = call_task(
        dag_module.get_latest_s3_uri, "s3://my-bucket/vendor", pattern=None
    )
    assert out == "s3://my-bucket/vendor/ds2/"


# ---------------------------------------------------------------------
# Unit tests: build_table_prefix accepts 'table' kw (prevents your runtime error)
# ---------------------------------------------------------------------
def test_build_table_prefix_accepts_table_kw(dag_module):
    out = call_task(
        dag_module.build_table_prefix,
        bucket_name="bucket",
        vendor="salesforce",
        dataset_id="ds1",
        testing=False,
        table="accounts",
    )
    assert out == "s3://bucket/salesforce/ds1/"


# ---------------------------------------------------------------------
# Unit tests: zipper helpers
# ---------------------------------------------------------------------
def test_zip_specs_with_events(dag_module):
    specs = [
        {"table": "a", "dataset_id": "1"},
        {"table": "b", "dataset_id": "2"},
    ]
    events = ['{"x":1}', '{"y":2}']
    out = call_task(dag_module.zip_specs_with_events, specs, events)
    assert out[0]["table"] == "a"
    assert out[0]["dataset_id"] == "1"
    assert out[0]["event_json"] == '{"x":1}'


def test_zip_tables_with_latest_path(dag_module):
    specs = [
        {"table": "a", "dataset_id": "1"},
        {"table": "b", "dataset_id": "2"},
    ]
    paths = ["s3://bucket/vendor/1/run=1/", "s3://bucket/vendor/2/run=2/"]
    out = call_task(dag_module.zip_tables_with_latest_path, specs, paths)
    assert out == [
        {"table_name": "a", "s3_uri": "s3://bucket/vendor/1/run=1/"},
        {"table_name": "b", "s3_uri": "s3://bucket/vendor/2/run=2/"},
    ]


# ---------------------------------------------------------------------
# DAG structure smoke test
# ---------------------------------------------------------------------
def test_dag_import_and_task_ids(dag_module):
    dag = dag_module.dag
    assert dag.dag_id == "debi_generic_ingester_glue_runner"

    expected = {
        "resolve_run_config",
        "reconcile_table_specs",
        "latest_framework_zip",
        "build_exchange_extras",
        "build_env_vars",
        "build_data_extras",
        "build_event",
        "zip_specs_with_events",
        "glue_op_kwargs",
        "run_glue_job",
        "table_prefix",
        "latest_path",
        "zip_tables_with_latest_path",
        "copy_sql",
        "load_table",
        "specs_to_table_names",
        "dedupe_sql",
        "dedupe_table",
    }
    assert expected.issubset(set(dag.task_ids))


def test_build_event_json_for_table_puts_dataset_id_inside_env_vars(dag_module):
    env_vars = {"REGION": "x", "BUCKET_NAME": "b"}
    exchange = None
    data_extras = None

    out = call_task(
        dag_module.build_event_json_for_table,
        vendor="salesforce",
        table="accounts",
        dataset_id="ds1",
        env_vars=env_vars,
        exchange_extras=exchange,
        data_extras=data_extras,
    )
    payload = json.loads(out)

    assert payload["vendor"] == "salesforce"
    assert payload["table"] == "accounts"

    assert payload["env_vars"]["dataset_id"] == "ds1"

    assert "dataset_id" not in payload

    assert "dataset_id" not in env_vars
