from __future__ import annotations

import fnmatch
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urlparse

import boto3
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import (
    SQLExecuteQueryOperator,
)
from dags.common.dag_utilities import (
    failover_managed_dag_tag,
    get_bucket_name,
    get_c1s_oauth_endpoint,
    get_shairflow_environment,
    get_shairflow_region,
    get_truncated_shairflow_region,
)
from dags.common.slack import task_fail_slack_alert
from dags.common.user_defined_filters import ts_nodash_to_YYYYMMDDHHmmss

LOG = logging.getLogger(__name__)
CURRENT_DIR = str(Path(__file__).resolve().parent)


def _deep_replace_placeholders(obj: Any, creds: Mapping[str, Any]) -> Any:
    """
    Replace placeholders like {{TOKEN}} in nested dict/list/str structures.

    IMPORTANT: If creds[key] is missing OR None, leave the placeholder intact
    (instead of turning it into the string "None").
    """
    ph = re.compile(r"\{\{(\w+)\}\}")

    def repl(match: re.Match) -> str:
        key = match.group(1)
        val = creds.get(key, None)
        return match.group(0) if val is None else str(val)

    if isinstance(obj, dict):
        return {k: _deep_replace_placeholders(v, creds) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_replace_placeholders(item, creds) for item in obj]
    if isinstance(obj, str):
        return ph.sub(repl, obj)
    return obj


@task
def resolve_run_config() -> dict:
    """
    Resolve everything that depends on dag_run.conf at runtime.

    Expected dag_run.conf shape (matches your scheduler):
      {
        "vendor": "salesforce" | "revCloud" | ...,
        "credentials": {...},
        "start_date": <any string> (optional),
        "end_date": <any string> (optional)
      }

    NOTE: start_date/end_date are passed through as-is because vendors may vary.

    SECURITY NOTE:
      - raw credentials are NOT returned in XCom (prevents leaking to logs/XCom UI)
      - data_extras placeholders are resolved in-task using credentials
    """
    ctx = get_current_context()
    conf = ctx["dag_run"].conf or {}

    vendor = conf.get("vendor")
    if not vendor:
        raise ValueError("dag_run.conf['vendor'] is required")

    vendor = str(vendor).lower()

    workflow_dict = (
        Variable.get(
            f"INGESTER_WORKFLOW_{vendor.upper()}",
            default_var={},
            deserialize_json=True,
        )
        or {}
    )
    if not isinstance(workflow_dict, dict):
        workflow_dict = {}

    tables_dict = workflow_dict.get("INGESTER_TABLES", {}) or {}
    if not isinstance(tables_dict, dict) or not tables_dict:
        raise ValueError(f"INGESTER_TABLES missing/empty for vendor={vendor}")

    tables: List[dict] = [
        {"table": t, "dataset_id": ds} for t, ds in tables_dict.items()
    ]

    # Debug-only counts (no secrets). Helps explain mapped-task explosions.
    unique_pairs = {
        (str(x.get("table", "")).lower(), str(x.get("dataset_id", "")).lower())
        for x in tables
    }
    LOG.warning(
        "resolve_run_config vendor=%s tables_count=%d unique_pairs=%d",
        vendor,
        len(tables),
        len(unique_pairs),
    )

    credentials = conf.get("credentials", {}) or {}
    if not isinstance(credentials, dict):
        credentials = {}

    start_date = conf.get("start_date") or workflow_dict.get(
        "INGESTER_START_DATE", "2000-01-01"
    )
    end_date = (
        conf.get("end_date")
        or workflow_dict.get("INGESTER_END_DATE")
        or datetime.now().strftime("%Y-%m-%d")
    )

    testing = bool(workflow_dict.get("INGESTER_TESTING", False))
    dedupe = bool(workflow_dict.get("INGESTER_DEDUPE", False))
    python_modules = workflow_dict.get("INGESTER_PYTHON_MODULE", None)
    etl_job_name = workflow_dict.get("INGESTER_GLUE_JOB_NAME", "etl-job")
    etl_conn_name = workflow_dict.get("INGESTER_GLUE_CONN_NAME", "etl-net-conn")
    run_mode = workflow_dict.get("INGESTER_RUN_MODE", "once")

    config_path = workflow_dict.get("INGESTER_CONFIG_PATH", None)
    repo_name = workflow_dict.get(
        "INGESTER_CONFIG_REPO_NAME", "config_management"
    )

    sql_params = workflow_dict.get("INGESTER_SQL_PARAMS", {}) or {}
    if not isinstance(sql_params, dict):
        sql_params = {}

    ingester_env_vars = workflow_dict.get("INGESTER_ENV_VARS", {}) or {}
    if not isinstance(ingester_env_vars, dict):
        ingester_env_vars = {}

    # Resolve data extras placeholders *here* so we don't push credentials into XCom.
    data_extras = workflow_dict.get("INGESTER_DATA_EXTRAS", None)
    if data_extras and credentials:
        data_extras = _deep_replace_placeholders(data_extras, credentials)

    exchange_enabled = bool(workflow_dict.get("INGESTER_EXCHANGE", False))

    return {
        "vendor": vendor,
        "tables": tables,
        # "credentials": credentials,  # intentionally NOT returned
        "start_date": start_date,
        "end_date": end_date,
        "testing": testing,
        "dedupe": dedupe,
        "python_modules": python_modules,
        "etl_job_name": etl_job_name,
        "etl_conn_name": etl_conn_name,
        "run_mode": run_mode,
        "config_path": config_path,
        "repo_name": repo_name,
        "sql_params": sql_params,
        "ingester_env_vars": ingester_env_vars,
        "data_extras": data_extras,
        "exchange_enabled": exchange_enabled,
    }


@task
def reconcile_tables(tables: List[dict], vendor: str) -> List[dict]:
    """
    Normalize + dedupe tables BEFORE mapping.

    Deduping is case-insensitive on (table, dataset_id), keeps first occurrence.
    """
    if not isinstance(tables, list):
        raise ValueError(f"tables must be a list, got {type(tables)}")

    seen: set[tuple[str, str]] = set()
    out: List[dict] = []

    for row in tables:
        if not isinstance(row, dict):
            continue

        t = str(row.get("table", "")).strip()
        ds = str(row.get("dataset_id", "")).strip()
        if not t or not ds:
            continue

        key = (t.lower(), ds.lower())
        if key in seen:
            continue

        seen.add(key)
        out.append({"table": t, "dataset_id": ds})

    if not out:
        raise ValueError(f"No valid tables after reconcile for vendor={vendor}")

    LOG.warning(
        "reconcile_tables vendor=%s in=%d out=%d", vendor, len(tables), len(out)
    )
    return out


@task
def get_latest_s3_uri(s3_prefix: str, pattern: Optional[str] = None) -> str:
    """
    Return s3://bucket/key for the newest object under s3_prefix.

    If pattern is provided, filter objects by fnmatch on the *filename* and return newest match.
    Otherwise, return newest *path* (common prefix) created under s3_prefix.
    """
    u = urlparse(s3_prefix)
    if u.scheme != "s3" or not u.netloc:
        raise ValueError(f"Expected s3://bucket/prefix, got: {s3_prefix}")

    bucket = u.netloc
    prefix = u.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    if pattern:
        matches: List[Dict[str, Any]] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                filename = key.rsplit("/", 1)[-1]
                if fnmatch.fnmatch(filename, pattern):
                    matches.append(obj)

        if not matches:
            raise ValueError(
                f"No objects found under {s3_prefix} matching {pattern!r}"
            )

        newest = max(matches, key=lambda o: o["LastModified"])
        return f"s3://{bucket}/{newest['Key']}"

    paths: Dict[str, datetime] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []) or []:
            path = common_prefix["Prefix"]
            path_objs: List[Dict[str, Any]] = []
            for sub_page in paginator.paginate(Bucket=bucket, Prefix=path):
                path_objs.extend(sub_page.get("Contents", []) or [])
            if path_objs:
                latest_in_path = max(path_objs, key=lambda o: o["LastModified"])
                paths[path] = latest_in_path["LastModified"]

    if not paths:
        raise ValueError(f"No paths found under {s3_prefix}")

    latest_path = max(paths.items(), key=lambda x: x[1])[0]
    return f"s3://{bucket}/{latest_path}"


@task
def get_sql(
    table_name: str,
    sql_params: dict,
    type: str,
    s3_uri: Optional[str] = None,
    enabled: bool = True,
) -> str:
    """
    Load a SQL template from: <CURRENT_DIR>/<type>/<type>_<table_name>.sql
    Apply replacements:
      - {{ params.target_table }} => <DATABASE>.<SCHEMA>.<table>
      - {{ params.s3_uri }}       => key-only (no s3://bucket/) if provided
    If enabled=False, return a safe no-op query.
    """
    if not enabled:
        return "SELECT 1;"

    file_name = f"{CURRENT_DIR}/{type}/{type}_{table_name}.sql"

    s3_key_only = re.sub(r"^s3://[^/]+/", "", s3_uri or "")
    table = f"{sql_params['DATABASE']}.{sql_params['SCHEMA']}.{table_name}"

    with open(file_name, "r") as f:
        tpl = f.read()

    sql_query = tpl.replace("{{ params.target_table }}", table)
    if s3_key_only:
        sql_query = sql_query.replace("{{ params.s3_uri }}", s3_key_only)

    return sql_query


@task
def build_exchange_extras(env: str) -> dict:
    c1_oauth_url = get_c1s_oauth_endpoint(env)

    exchange_id = Variable.get(
        "C1SCOREDATASERVICES_EXCHANGE_ID", default_var=None
    )
    exchange_secret = Variable.get(
        "C1SCOREDATASERVICES_EXCHANGE_SECRET", default_var=None
    )

    return {
        "c1_oauth_url": c1_oauth_url,
        "exchange_headers": {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        "exchange_data": {
            "client_id": exchange_id,
            "client_secret": exchange_secret,
            "grant_type": "client_credentials",
        },
    }


@task
def build_env_vars(
    region: str, bucket_name: str, ingester_env_vars: dict
) -> dict:
    env_vars = dict(ingester_env_vars or {})
    env_vars["REGION"] = region
    env_vars["BUCKET_NAME"] = bucket_name
    return env_vars


@task
def build_event_json_for_table(
    vendor: str,
    table: str,
    dataset_id: str,
    env_vars: dict,
    exchange_extras: dict | None,
    data_extras: Any,
) -> str:
    base_event: Dict[str, Any] = {
        "env_vars": env_vars,
        "table": table,
        "vendor": vendor,
        "dataset_id": dataset_id,
    }
    if exchange_extras:
        base_event.update(exchange_extras)
    if data_extras:
        base_event.update(data_extras)
    return json.dumps(base_event)


@task
def build_table_prefix(
    bucket_name: str, vendor: str, dataset_id: str, testing: bool
) -> str:
    if testing:
        return f"s3://{bucket_name}/test/{vendor}/{dataset_id}"
    return f"s3://{bucket_name}/{vendor}/{dataset_id}/"


@task
def build_glue_operator_kwargs(
    env: str,
    region: str,
    vendor: str,
    table: str,
    dataset_id: str,
    event_json: str,
    latest_zip_s3: str,
    etl_job_name: str,
    etl_conn_name: str,
    run_mode: str,
    repo_name: str,
    config_path: str,
    github_token: str,
    start_date: str,
    end_date: str,
    python_modules: Optional[str],
    index_url: str,
) -> dict:
    """
    Build a single GlueJobOperator kwargs dict for dynamic mapping via expand_kwargs().
    """
    script_args = {
        "--env": env,
        "--run_mode": run_mode,
        "--table": table,
        "--vendor": vendor,
        "--repo_name": repo_name,
        "--file_path": config_path,
        "--github_token": github_token,
        "--start_date": start_date,
        "--end_date": end_date,
        "--additional-python-modules": python_modules,
        "--python-modules-installer-option": f"--index-url={index_url}",
        "--extra-py-files": latest_zip_s3,
        "--event": event_json,
    }
    script_args = {k: v for k, v in script_args.items() if v is not None}

    return {
        "task_id": f"run_glue_job__{table}",
        "job_name": etl_job_name,
        "aws_conn_id": etl_conn_name,
        "region_name": region,
        "script_args": script_args,
        "wait_for_completion": True,
    }


@dag(
    tags=[
        "invoke-lambda",
        "airflow-2.x.x-compatible",
        failover_managed_dag_tag(),
    ],
    default_args={
        "owner": "airflow",
        "retries": 5,
        "retry_delay": pendulum.duration(minutes=5),
        "depends_on_past": False,
        "on_failure_callback": task_fail_slack_alert,
    },
    description="DAG that invokes the generic glue ingester",
    dag_id="debi_generic_ingester_glue_runner",
    catchup=False,
    user_defined_filters={"convertToEpochSeconds": ts_nodash_to_YYYYMMDDHHmmss},
    max_active_runs=1,
    start_date=datetime(2023, 12, 23, tzinfo=pendulum.timezone("UTC")),
)
def generic_ingester_dag():
    env = get_shairflow_environment().lower()
    region = get_shairflow_region().lower()
    truncated_region = get_truncated_shairflow_region()
    bucket_name = get_bucket_name(env, truncated_region)

    cfg = resolve_run_config()
    vendor = cfg["vendor"]

    clean_tables = reconcile_tables(cfg["tables"], vendor)

    # latest framework zip (single task)
    zip_prefix = f"s3://{bucket_name}/code/ETL/"
    latest_zip = get_latest_s3_uri.override(task_id="latest_framework_zip")(
        s3_prefix=zip_prefix,
        pattern="debi-etl-framework-glue*.zip",
    )

    exchange_extras = (
        build_exchange_extras(env) if cfg["exchange_enabled"] else None
    )
    env_vars = build_env_vars(region, bucket_name, cfg["ingester_env_vars"])
    data_extras = cfg["data_extras"]

    events = (
        build_event_json_for_table.override(task_id="build_event")
        .partial(
            vendor=vendor,
            env_vars=env_vars,
            exchange_extras=exchange_extras,
            data_extras=data_extras,
        )
        .expand_kwargs(clean_tables)
    )

    github_token = Variable.get(
        "C1SCOREDATASERVICES_GITHUB_PASSWORD", default_var=None
    )

    glue_kwargs = (
        build_glue_operator_kwargs.override(task_id="glue_op_kwargs")
        .partial(
            env=env,
            region=region,
            vendor=vendor,
            latest_zip_s3=latest_zip,
            etl_job_name=cfg["etl_job_name"],
            etl_conn_name=cfg["etl_conn_name"],
            run_mode=cfg["run_mode"],
            repo_name=cfg["repo_name"],
            config_path=cfg["config_path"],
            github_token=github_token,
            start_date=cfg["start_date"],
            end_date=cfg["end_date"],
            python_modules=cfg["python_modules"],
            index_url="https://artifactory.cloud.capitalone.com/artifactory/api/pypi/pypi-internalfacing/simple",
        )
        .expand_kwargs(
            [
                {
                    "table": t["table"],
                    "dataset_id": t["dataset_id"],
                    "event_json": events,
                }
                for t in clean_tables
            ]
        )
    )

    run_glue = GlueJobOperator.partial(task_id="run_glue_job").expand_kwargs(
        glue_kwargs
    )

    prefixes = (
        build_table_prefix.override(task_id="table_prefix")
        .partial(bucket_name=bucket_name, vendor=vendor, testing=cfg["testing"])
        .expand_kwargs(clean_tables)
    )

    latest_json = (
        get_latest_s3_uri.override(task_id="latest_json")
        .partial(pattern=None)
        .expand(s3_prefix=prefixes)
    )

    copy_sql = (
        get_sql.override(task_id="copy_sql")
        .partial(sql_params=cfg["sql_params"], type="copy", enabled=True)
        .expand_kwargs(
            [
                {"table_name": t["table"], "s3_uri": latest_json}
                for t in clean_tables
            ]
        )
    )

    load_table = SQLExecuteQueryOperator.partial(
        task_id="load_table",
        conn_id="snowflake_salesforce",
    ).expand(sql=copy_sql)

    dedupe_sql = (
        get_sql.override(task_id="dedupe_sql")
        .partial(
            sql_params=cfg["sql_params"],
            type="deduplication",
            s3_uri=None,
            enabled=cfg["dedupe"],
        )
        .expand_kwargs([{"table_name": t["table"]} for t in clean_tables])
    )

    dedupe_table = SQLExecuteQueryOperator.partial(
        task_id="dedupe_table",
        conn_id="snowflake_salesforce",
    ).expand(sql=dedupe_sql)

    latest_zip >> events >> glue_kwargs >> run_glue
    (
        run_glue
        >> prefixes
        >> latest_json
        >> copy_sql
        >> load_table
        >> dedupe_sql
        >> dedupe_table
    )


dag = generic_ingester_dag()
