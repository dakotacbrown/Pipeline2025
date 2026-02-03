# tests/dags/test_debi_generic_ingester_glue_runner.py
from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pytest

# ---------------------------------------------------------------------
# Configure these for your repo
# ---------------------------------------------------------------------
DAG_FILE_RELATIVE_PATH = Path(
    "dags/generic/debi_generic_ingester_glue_runner.py"
)
DAG_MODULE_NAME = "debi_generic_ingester_glue_runner_under_test"


# ---------------------------------------------------------------------
# Utilities: safe dynamic import with dependency stubs
# ---------------------------------------------------------------------
def _project_root() -> Path:
    """
    Assumes tests live in: <repo>/tests/dags/test_*.py
    """
    return Path(__file__).resolve().parents[2]


def _install_stub_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Provide minimal stub modules so importing the DAG file doesn't require
    your full dags.common package to be importable in the test environment.
    """
    # dags
    dags_mod = types.ModuleType("dags")
    common_mod = types.ModuleType("dags.common")

    dag_utils_mod = types.ModuleType("dags.common.dag_utilities")
    slack_mod = types.ModuleType("dags.common.slack")
    udf_mod = types.ModuleType("dags.common.user_defined_filters")

    # ---- stubs used at PARSE TIME in the DAG factory ----
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

    # ---- slack callback stub ----
    def task_fail_slack_alert(*args: Any, **kwargs: Any) -> None:
        return None

    slack_mod.task_fail_slack_alert = task_fail_slack_alert

    # ---- user_defined_filters stub ----
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
def dag_module(monkeypatch):
    file_path = _project_root() / DAG_FILE_RELATIVE_PATH
    mod = _load_module_from_path(DAG_MODULE_NAME, file_path, monkeypatch)
    try:
        yield mod
    finally:
        sys.modules.pop(DAG_MODULE_NAME, None)


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
    assert out["b"][1] == "{{MISSING}}"  # missing key stays
    assert out["b"][2]["c"] == "{{NONEVAL}}"  # None stays
    assert out["d"] == 123


def test_deep_replace_placeholders_non_str_passthrough(dag_module):
    fn = dag_module._deep_replace_placeholders
    assert fn(5, {"X": "y"}) == 5
    assert fn(None, {"X": "y"}) is None


# ---------------------------------------------------------------------
# Unit tests: extract_table_names / extract_dataset_ids
# ---------------------------------------------------------------------
def test_extract_helpers(dag_module):
    tables = [
        {"table": "t1", "dataset_id": "d1"},
        {"table": "t2", "dataset_id": "d2"},
    ]
    assert dag_module.extract_table_names(tables) == ["t1", "t2"]
    assert dag_module.extract_dataset_ids(tables) == ["d1", "d2"]


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
        dag_module.resolve_run_config()


def test_resolve_run_config_valid(monkeypatch, dag_module):
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

    # Variable.get should return workflow dict for INGESTER_WORKFLOW_SALESFORCE
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
            }
        return default_var

    monkeypatch.setattr(dag_module.Variable, "get", fake_variable_get)

    cfg = dag_module.resolve_run_config()

    assert cfg["vendor"] == "salesforce"
    assert cfg["tables"] == [
        {"table": "accounts", "dataset_id": "ds1"},
        {"table": "contacts", "dataset_id": "ds2"},
    ]
    assert cfg["credentials"] == {"TOKEN": "t"}
    assert cfg["start_date"] == "2020-01-01"
    assert cfg["end_date"] == "2020-01-31"
    assert cfg["testing"] is True
    assert cfg["dedupe"] is False
    assert cfg["etl_job_name"] == "jobname"
    assert cfg["etl_conn_name"] == "connname"
    assert cfg["repo_name"] == "repo"
    assert cfg["config_path"] == "path/to/config.yml"
    assert cfg["sql_params"] == {"DATABASE": "DB", "SCHEMA": "SC"}
    assert cfg["ingester_env_vars"] == {"A": "B"}
    assert cfg["data_extras"] == {"hello": "{{TOKEN}}"}


def test_resolve_run_config_tables_missing_raises(monkeypatch, dag_module):
    def fake_ctx():
        return {"dag_run": _FakeDagRun(conf={"vendor": "x"})}

    monkeypatch.setattr(dag_module, "get_current_context", fake_ctx)

    def fake_variable_get(key: str, default_var=None, deserialize_json=False):
        if key == "INGESTER_WORKFLOW_X":
            return {"INGESTER_TABLES": {}}
        return default_var

    monkeypatch.setattr(dag_module.Variable, "get", fake_variable_get)

    with pytest.raises(ValueError, match=r"INGESTER_TABLES missing/empty"):
        dag_module.resolve_run_config()


# ---------------------------------------------------------------------
# Unit tests: get_latest_s3_uri (mock boto3 paginator)
# ---------------------------------------------------------------------
class _FakePaginator:
    def __init__(self, pages: List[dict]):
        self._pages = pages
        self.calls: List[dict] = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        for p in self._pages:
            yield p


class _FakeS3Client:
    def __init__(self, paginator: _FakePaginator):
        self._paginator = paginator

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return self._paginator


def test_get_latest_s3_uri_invalid_scheme_raises(dag_module):
    with pytest.raises(ValueError, match=r"Expected s3://bucket/prefix"):
        dag_module.get_latest_s3_uri("https://example.com/x")


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
    paginator = _FakePaginator(pages)
    fake_s3 = _FakeS3Client(paginator)

    monkeypatch.setattr(dag_module.boto3, "client", lambda name: fake_s3)

    out = dag_module.get_latest_s3_uri(
        "s3://my-bucket/code/ETL", pattern="debi-etl-framework-glue*.zip"
    )
    assert out == "s3://my-bucket/code/ETL/debi-etl-framework-glue-2.zip"


def test_get_latest_s3_uri_prefix_branch_latest_common_prefix(
    monkeypatch, dag_module
):
    """
    When pattern is None, code finds "latest path" among CommonPrefixes by scanning
    objects inside each prefix and taking the prefix that contains the newest object.
    """
    dt_old = datetime(2024, 1, 1, tzinfo=timezone.utc)
    dt_new = datetime(2024, 1, 5, tzinfo=timezone.utc)

    # First call (Delimiter="/") returns CommonPrefixes
    # Subsequent calls scan each prefix and return Contents for that prefix.
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

    paginator = _FakePaginator(pages_for_delimiter)
    fake_s3 = _FakeS3Client(paginator)

    def fake_paginate(**kwargs):
        # route based on Prefix
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

    # monkeypatch paginator.paginate to be smarter
    paginator.paginate = fake_paginate  # type: ignore[assignment]

    monkeypatch.setattr(dag_module.boto3, "client", lambda name: fake_s3)

    out = dag_module.get_latest_s3_uri("s3://my-bucket/vendor", pattern=None)
    assert out == "s3://my-bucket/vendor/ds2/"


# ---------------------------------------------------------------------
# Unit tests: get_sql
# ---------------------------------------------------------------------
def test_get_sql_replacements(tmp_path: Path, monkeypatch, dag_module):
    # Create fake sql template under CURRENT_DIR/type/type_table.sql
    current_dir = tmp_path
    (current_dir / "copy").mkdir()
    sql_file = current_dir / "copy" / "copy_accounts.sql"
    sql_file.write_text(
        "COPY INTO {{ params.target_table }} FROM '{{ params.s3_uri }}';"
    )

    monkeypatch.setattr(dag_module, "CURRENT_DIR", str(current_dir))

    sql_params = {"DATABASE": "DB", "SCHEMA": "SC"}
    s3_uri = "s3://bucket/vendor/ds/file.json"

    out = dag_module.get_sql(
        "accounts", sql_params=sql_params, type="copy", s3_uri=s3_uri
    )
    assert "DB.SC.accounts" in out
    assert "vendor/ds/file.json" in out  # key-only (no s3://bucket/)


def test_get_sql_disabled_returns_noop(tmp_path: Path, monkeypatch, dag_module):
    monkeypatch.setattr(dag_module, "CURRENT_DIR", str(tmp_path))
    out = dag_module.get_sql(
        "anything",
        sql_params={"DATABASE": "DB", "SCHEMA": "SC"},
        type="copy",
        enabled=False,
    )
    assert out == "SELECT 1;"


# ---------------------------------------------------------------------
# Unit tests: build_exchange_extras / build_env_vars / build_data_extras / event json / prefix / glue kwargs
# ---------------------------------------------------------------------
def test_build_exchange_extras(monkeypatch, dag_module):
    monkeypatch.setattr(
        dag_module, "get_c1s_oauth_endpoint", lambda env: f"https://oauth/{env}"
    )

    def fake_variable_get(key: str, default_var=None):
        if key == "C1SCOREDATASERVICES_EXCHANGE_ID":
            return "cid"
        if key == "C1SCOREDATASERVICES_EXCHANGE_SECRET":
            return "csec"
        return default_var

    monkeypatch.setattr(dag_module.Variable, "get", fake_variable_get)

    out = dag_module.build_exchange_extras("dev")
    assert out["c1_oauth_url"] == "https://oauth/dev"
    assert out["exchange_data"]["client_id"] == "cid"
    assert out["exchange_data"]["client_secret"] == "csec"
    assert out["exchange_data"]["grant_type"] == "client_credentials"


def test_build_env_vars(dag_module):
    out = dag_module.build_env_vars("us-east-1", "bucket", {"X": "Y"})
    assert out["X"] == "Y"
    assert out["REGION"] == "us-east-1"
    assert out["BUCKET_NAME"] == "bucket"


def test_build_data_extras_placeholder_replacement(dag_module):
    data_extras = {"headers": {"Authorization": "Bearer {{TOKEN}}"}}
    creds = {"TOKEN": "abc"}
    out = dag_module.build_data_extras(data_extras, creds)
    assert out["headers"]["Authorization"] == "Bearer abc"


def test_build_event_json_for_table_merges(dag_module):
    env_vars = {"REGION": "x"}
    exchange = {"c1_oauth_url": "u", "exchange_data": {"a": "b"}}
    data_extras = {"hello": "world"}

    out = dag_module.build_event_json_for_table(
        vendor="salesforce",
        table="accounts",
        dataset_id="ds1",
        env_vars=env_vars,
        exchange_extras=exchange,
        data_extras=data_extras,
    )
    # It returns a JSON string; check key bits exist
    assert '"vendor": "salesforce"' in out
    assert '"table": "accounts"' in out
    assert '"dataset_id": "ds1"' in out
    assert '"c1_oauth_url": "u"' in out
    assert '"hello": "world"' in out


def test_build_table_prefix(dag_module):
    assert (
        dag_module.build_table_prefix(
            "bucket", "salesforce", "ds1", testing=False
        )
        == "s3://bucket/salesforce/ds1/"
    )
    assert (
        dag_module.build_table_prefix(
            "bucket", "salesforce", "ds1", testing=True
        )
        == "s3://bucket/test/salesforce/ds1"
    )


def test_build_glue_operator_kwargs_drops_none_values(dag_module):
    out = dag_module.build_glue_operator_kwargs(
        env="dev",
        region="us-east-1",
        vendor="salesforce",
        table="accounts",
        dataset_id="ds1",
        event_json='{"x":1}',
        latest_zip_s3="s3://b/code/ETL/z.zip",
        etl_job_name="job",
        etl_conn_name="conn",
        run_mode="once",
        repo_name="repo",
        config_path="path.yml",
        github_token="gh",
        start_date="2020-01-01",
        end_date="2020-01-31",
        python_modules=None,  # should be removed
        index_url="https://index/simple",
    )

    assert out["task_id"] == "run_glue_job__accounts"
    assert out["job_name"] == "job"
    assert out["aws_conn_id"] == "conn"
    assert out["region_name"] == "us-east-1"
    assert out["wait_for_completion"] is True

    script_args = out["script_args"]
    assert script_args["--env"] == "dev"
    assert script_args["--table"] == "accounts"
    assert script_args["--vendor"] == "salesforce"
    assert script_args["--extra-py-files"] == "s3://b/code/ETL/z.zip"
    assert script_args["--event"] == '{"x":1}'
    assert "--additional-python-modules" not in script_args  # dropped


# ---------------------------------------------------------------------
# DAG structure test (import DAG and check task ids exist)
# ---------------------------------------------------------------------
def test_dag_import_and_task_ids(dag_module):
    dag = dag_module.dag
    assert dag.dag_id == "debi_generic_ingester_glue_runner"

    # A few key tasks that should always exist (even with dynamic mapping)
    # Note: overridden task_ids are the ones you set via .override(task_id="...")
    expected = {
        "resolve_run_config",
        "extract_table_names",
        "extract_dataset_ids",
        "latest_framework_zip",
        "build_exchange_extras",
        "build_env_vars",
        "build_data_extras",
        "build_event",
        "glue_op_kwargs",
        "run_glue_job",
        "table_prefix",
        "latest_json",
        "copy_sql",
        "load_table",
        "dedupe_sql",
        "dedupe_table",
    }

    assert expected.issubset(set(dag.task_ids))
