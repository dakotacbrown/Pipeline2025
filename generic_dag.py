# generic_ingester.py
from __future__ import annotations

import fnmatch
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urlparse

import boto3
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import (
    SQLExecuteQueryOperator,
)
from dags.common.dag_utilities import (
    failover_managed_dag_tag,
    get_bucket_name,
    get_cls_oauth_endpoint,
    get_shairflow_environment,
    get_shairflow_region,
    get_truncated_shairflow_region,
)
from dags.common.slack import task_fail_slack_alert
from dags.common.user_defined_filters import ts_nodash_to_YYYYMMDDHHmmss

# Needed for standalone to avoid potential deadlock during connection init
print(f"SnowflakeHook.conn_type: {SnowflakeHook.conn_type!r}")
CURRENT_DIR = os.environ["AIRFLOW__CORE__DAGS_FOLDER"] + "/dags/generic"


def _safe_task_id(s: str) -> str:
    """Airflow task_ids: letters/numbers/_ only."""
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", str(s))
    return s.strip("_").lower()


def _deep_replace_placeholders(obj: Any, creds: Mapping[str, Any]) -> Any:
    PH = re.compile(r"\{\{(\w+)\}\}")
    if isinstance(obj, dict):
        return {k: _deep_replace_placeholders(v, creds) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_replace_placeholders(item, creds) for item in obj]
    if isinstance(obj, str):
        return PH.sub(lambda m: creds.get(m.group(1), m.group(0)), obj)
    return obj


@task
def get_copy_sql(table_name: str, sql_params: dict, s3_uri: str) -> str:
    file_name = f"{CURRENT_DIR}/copy/copy_{table_name}.sql"
    s3_uri = re.sub(r"^s3://[^/]+/", "", s3_uri or "")
    table = f"{sql_params['DATABASE']}.{sql_params['SCHEMA']}.{table_name}"
    with open(file_name, "r") as file:
        tpl = file.read()
    sql_query = tpl.replace("{{ params.target_table }}", table)
    sql_query = sql_query.replace("{{ params.s3_url }}", s3_uri)
    return sql_query


@task
def get_latest_s3_uri(s3_prefix: str, pattern: Optional[str] = None) -> str:
    """
    Return s3://bucket/key for the newest object under s3_prefix.

    If pattern is provided, filter objects by fnmatch on the *filename* and return the file.
    Otherwise, return the latest *path* (prefix) created under s3_prefix.
    """
    u = urlparse(s3_prefix)
    if u.scheme != "s3" or not u.netloc:
        raise ValueError(f"Expected s3://bucket/prefix, got: {s3_prefix!r}")

    bucket = u.netloc
    prefix = u.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    if pattern:
        # Pattern provided: return the newest matching file
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

    else:
        # No pattern: return the latest path (prefix) created
        paths: Dict[str, datetime] = {}
        for page in paginator.paginate(
            Bucket=bucket, Prefix=prefix, Delimiter="/"
        ):
            for common_prefix in page.get("CommonPrefixes", []) or []:
                path = common_prefix["Prefix"]

                # Get the most recent object in this path to determine path's timestamp
                path_objs = []
                for sub_page in paginator.paginate(Bucket=bucket, Prefix=path):
                    path_objs.extend(sub_page.get("Contents", []) or [])
                if path_objs:
                    latest_in_path = max(
                        path_objs, key=lambda o: o["LastModified"]
                    )
                    paths[path] = latest_in_path["LastModified"]

        if not paths:
            raise ValueError(f"No paths found under {s3_prefix!r}")

        latest_path = max(paths.items(), key=lambda x: x[1])[0]
        return f"s3://{bucket}/{latest_path}"


@dag(
    tags=[
        "invoke-lambda",
        "airflow-2.x.x-compatible",
        failover_managed_dag_tag(),
    ],
    default_args={
        "owner": "Airflow",
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
    doc_md="""
Generic Ingester Glue DAG orchestrates the following
1. Collection of raw data
2. Storage of raw data to S3
3. Load latest json into Snowflake (optional)
""",
)
def generic_ingester_dag():
    # --------------------
    # Parse-time config
    # --------------------
    env = get_shairflow_environment().lower()
    region = get_shairflow_region().lower()
    truncated_region = get_truncated_shairflow_region()
    bucket_name = get_bucket_name(env, truncated_region)

    # Note: must exist in dags.common.dag_utilities with this exact name
    cl_oauth_url = get_cls_oauth_endpoint(env)

    # Extract vendor from triggering DAG ID
    context = get_current_context()
    conf = context["dag_run"].conf
    vendor = conf.get("vendor", "generic").lower()

    workflow_dict = Variable.get(
        f"INGESTER_WORKFLOW_{vendor.upper()}",
        default_var={},
        deserialize_json=True,
    )
    if not isinstance(workflow_dict, dict):
        workflow_dict = {}

    testing = workflow_dict.get("INGESTER_TESTING", False)
    python_modules = workflow_dict.get("INGESTER_PYTHON_MODULES", None)
    etl_job_name = workflow_dict.get("INGESTER_GLUE_JOB_NAME", "etl-job")
    etl_conn_name = workflow_dict.get("INGESTER_GLUE_CONN_NAME", "etl-net-conn")
    run_mode = workflow_dict.get("INGESTER_RUN_MODE", "once")

    github_token = Variable.get("CISCOREDATASERVICES_GITHUB_PASSWORD", None)
    config_path = workflow_dict.get("INGESTER_CONFIG_PATH", None)
    repo_name = workflow_dict.get(
        "INGESTER_CONFIG_REPO_NAME", "config_management"
    )

    tables_dict = workflow_dict.get("INGESTER_TABLES", {})
    tables = list(tables_dict.keys())
    if not isinstance(tables, list):
        tables = []

    start_date = conf.get("start_date") or workflow_dict.get(
        "INGESTER_START_DATE", "2000-01-01"
    )
    end_date = conf.get("end_date") or workflow_dict.get(
        "INGESTER_END_DATE", datetime.now().strftime("%Y-%m-%d")
    )

    env_vars = workflow_dict.get("INGESTER_ENV_VARS", {})
    if not isinstance(env_vars, dict):
        env_vars = {}
    env_vars["REGION"] = region
    env_vars["BUCKET_NAME"] = bucket_name
    env_vars_with_region = env_vars.copy()

    sql_params = workflow_dict.get("INGESTER_SQL_PARAMS", {})
    if not isinstance(sql_params, dict):
        sql_params = {}

    exchange = workflow_dict.get("INGESTER_EXCHANGE", None)
    if exchange:
        ciscoredataservices_exchange_id = Variable.get(
            "CISCOREDATASERVICES_EXCHANGE_ID", None
        )
        ciscoredataservices_exchange_secret = Variable.get(
            "CISCOREDATASERVICES_EXCHANGE_SECRET", None
        )
        exchange_extras: Dict[str, Any] = {
            "cl_oauth_url": cl_oauth_url,
            "exchange_headers": {
                "Content-Type": "application/x-www-form-urlencoded"
            },
            "exchange_data": {
                "client_id": ciscoredataservices_exchange_id,
                "client_secret": ciscoredataservices_exchange_secret,
                "grant_type": "client_credentials",
            },
        }
    else:
        exchange_extras = {}

    data_extras = workflow_dict.get("INGESTER_DATA_EXTRAS", None)
    credentials = conf.get("credentials", {})
    if data_extras and credentials:
        data_extras = _deep_replace_placeholders(data_extras, credentials)

    # -------------------------
    # Task: build event JSON per table
    # -------------------------
    @task
    def build_event_json_for_table(table: str, dataset_id: str) -> str:
        base_event: Dict[str, Any] = {
            "env_vars": env_vars_with_region,
            "table": table,
            "vendor": vendor,
            "dataset_id": dataset_id,
        }
        if exchange_extras:
            base_event.update(exchange_extras)
        if data_extras:
            base_event.update(data_extras)
        return json.dumps(base_event)

    # -------------------------
    # Task: latest framework zip
    # -------------------------
    zip_prefix = f"s3://{bucket_name}/code/ETL/"
    latest_zip = get_latest_s3_uri.override(task_id="latest_framework_zip")(
        s3_prefix=zip_prefix,
        pattern="debi-etl-framework-glue*.zip",
    )

    # -------------------------
    # Per-table tasks
    # -------------------------
    for table in tables:
        dataset_id = tables_dict[table]
        safe = _safe_task_id(table)

        build_event = build_event_json_for_table.override(
            task_id=f"build_event_{safe}"
        )(table=table, dataset_id=dataset_id)

        run_glue = GlueJobOperator(
            task_id=f"run_glue_job_{safe}",
            job_name=etl_job_name,
            aws_conn_id=etl_conn_name,
            region_name=region,
            script_args={
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
                "--python-modules-installer-option": (
                    "--index-url=https://artifactory.cloud.capitalone.com/"
                    "artifactory/api/pypi/pypi-internalfacing/simple"
                ),
                "--extra-py-files": "{{ ti.xcom_pull(task_ids='latest_framework_zip') }}",
                "--event": f"{{{{ ti.xcom_pull(task_ids='build_event_{safe}') }}}}",
            },
            wait_for_completion=True,
        )

        # Latest jsonl under the table prefix
        if testing:
            table_prefix = f"s3://{bucket_name}/test/{vendor}/{dataset_id}"
        else:
            table_prefix = f"s3://{bucket_name}/{vendor}/{dataset_id}/"

        latest_jsonl = get_latest_s3_uri.override(
            task_id=f"latest_jsonl_{safe}"
        )(
            s3_prefix=table_prefix,
            pattern="*.jsonl",
        )

        copy_sql = get_copy_sql.override(task_id=f"copy_sql_{safe}")(
            table_name=table,
            sql_params=sql_params,
            s3_uri=latest_jsonl,
        )

        load_table = SQLExecuteQueryOperator(
            task_id=f"load_table_{safe}",
            conn_id="snowflake_salesforce",
            sql=copy_sql,
        )

        # Dependencies:
        latest_zip >> build_event >> run_glue
        run_glue >> latest_jsonl >> copy_sql >> load_table


dag = generic_ingester_dag()
