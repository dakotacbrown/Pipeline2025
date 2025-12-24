from __future__ import annotations

import fnmatch
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import boto3
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
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

# Needed for standalone to avoid potential deadlock during connection init
print(f"SnowflakeHook.conn_type: {SnowflakeHook.conn_type!r}")


def _safe_task_id(s: str) -> str:
    """Airflow task_ids: letters/numbers/_ only."""
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", str(s))
    return s.strip("_").lower()


def generate_params(
    sql_params: Dict[str, Any],
    table: str,
    latest_s3_uri_task_id: str,
) -> Dict[str, str]:
    """
    Generate parameters for Snowflake SQL tasks.

    - target_table: fully qualified target table
    - s3_uri: templated XCom pull from the "latest s3 uri" task

    NOTE: We intentionally keep `s3_prefix` as an alias for backward compatibility
    in case the SQL template references params.s3_prefix.
    """
    database = sql_params.get("DATABASE", "VALIDATION")
    schema = sql_params.get("SCHEMA", "PUBLIC")

    s3_uri_templated = (
        f"{{{{ ti.xcom_pull(task_ids='{latest_s3_uri_task_id}') }}}}"
    )

    return {
        "target_table": f"{database}.{schema}.{table}",
        "s3_uri": s3_uri_templated,
        "s3_prefix": s3_uri_templated,  # backward compat alias
    }


@task
def get_latest_s3_uri(s3_prefix: str, pattern: Optional[str] = None) -> str:
    """
    Return s3://bucket/key for the newest object under s3_prefix.

    If pattern is provided, filter objects by fnmatch on the *filename* (not full key).
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

    matches: List[Dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]

            if pattern:
                if fnmatch.fnmatch(filename, pattern):
                    matches.append(obj)
            else:
                matches.append(obj)

    if not matches:
        raise ValueError(
            f"No objects found under {s3_prefix}"
            + (f" matching {pattern!r}" if pattern else "")
        )

    newest = max(matches, key=lambda o: o["LastModified"])
    return f"s3://{bucket}/{newest['Key']}"


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
    description="DAG that invokes the salesforce glue ingester",
    dag_id="debi_ingester_glue_runner",
    schedule=None,
    catchup=False,
    user_defined_filters={"convertToEpochSeconds": ts_nodash_to_YYYYMMDDHHmmss},
    max_active_runs=1,
    start_date=datetime(2023, 12, 23, tzinfo=pendulum.timezone("UTC")),
    doc_md="""
Salesforce Ingester Glue DAG orchestrates the following
1. Collection of raw data
2. Storage of raw data to S3
3. Load latest parquet into Snowflake (optional)
""",
)
def salesforce_ingester_dag():
    # ----------------------------
    # Parse-time config
    # ----------------------------
    env = get_shairflow_environment().lower()
    region = get_shairflow_region().lower()
    truncated_region = get_truncated_shairflow_region()
    bucket_name = get_bucket_name(env, truncated_region)

    # Note: must exist in dags.common.dag_utilities with this exact name
    c1_oauth_url = get_c1s_oauth_endpoint(env)

    vendor = "salesforce"

    workflow_dict = Variable.get(
        f"INGESTER_WORKFLOW_{vendor.upper()}",
        default_var={},
        deserialize_json=True,
    )
    if not isinstance(workflow_dict, dict):
        workflow_dict = {}

    python_modules = workflow_dict.get("INGESTER_PYTHON_MODULE", None)
    etl_job_name = workflow_dict.get("INGESTER_GLUE_JOB_NAME", "etl-job")
    etl_conn_name = workflow_dict.get("INGESTER_GLUE_CONN_NAME", "etl-net-conn")
    run_mode = workflow_dict.get("INGESTER_RUN_MODE", "once")

    tables = workflow_dict.get("INGESTER_TABLES", [])
    if not isinstance(tables, list):
        tables = []

    config_path = workflow_dict.get("INGESTER_CONFIG_PATH", None)
    repo_name = workflow_dict.get(
        "INGESTER_CONFIG_REPO_NAME", "config_management"
    )
    start_date = workflow_dict.get("INGESTER_START_DATE", "2000-01-01")
    end_date = workflow_dict.get(
        "INGESTER_END_DATE", datetime.now().strftime("%Y-%m-%d")
    )

    env_vars = workflow_dict.get("INGESTER_ENV_VARS", {})
    if not isinstance(env_vars, dict):
        env_vars = {}
    env_vars["REGION"] = region
    env_vars["BUCKET_NAME"] = bucket_name
    env_vars_with_region = env_vars.copy()

    # Ensure we never pass None into Glue args (Glue/botocore will reject None)
    github_token = Variable.get("C1SCOREDATASERVICES_GITHUB_PASSWORD", None)

    c1scoredataservices_exchange_id = Variable.get(
        "C1SCOREDATASERVICES_EXCHANGE_ID", None
    )
    c1scoredataservices_exchange_secret = Variable.get(
        "C1SCOREDATASERVICES_EXCHANGE_SECRET", None
    )

    username = Variable.get("C1S_SALESFORCE_USERNAME", None)
    password = Variable.get("C1S_SALESFORCE_PASSWORD", None)
    client_id = Variable.get("C1S_SALESFORCE_CLIENTID", None)
    client_secret = Variable.get("C1S_SALESFORCE_CLIENTSECRET", None)

    copy_sql = Variable.get("INGESTER_COPY_SQL", None)

    sql_params = workflow_dict.get("INGESTER_SQL_PARAMS", {})
    if not isinstance(sql_params, dict):
        sql_params = {}

    if copy_sql:
        # Ensure we have a valid Snowflake connection
        hook = SnowflakeHook(snowflake_conn_id="snowflake_salesforce")
        if not hook.get_conn():
            raise ValueError(
                "Snowflake connection 'snowflake_salesforce' is not configured."
            )

    # ----------------------------
    # Auth + extras (kept)
    # ----------------------------
    data_auth_url = (
        "https://partner-apis-it.cloud.capitalone.com/third-party/salesforce/services/oauth2/token"
        if env == "qa"
        else "https://partner-apis.cloud.capitalone.com/third-party/salesforce/services/oauth2/token"
    )

    exchange_extras: Dict[str, Any] = {
        "c1_oauth_url": c1_oauth_url,
        "exchange_headers": {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        "exchange_data": {
            "client_id": c1scoredataservices_exchange_id,
            "client_secret": c1scoredataservices_exchange_secret,
            "grant_type": "client_credentials",
        },
    }

    data_extras: Dict[str, Any] = {
        "data_headers": {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json;v=1",
        },
        "data_auth": {
            "username": username,
            "password": password,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "password",
        },
        "data_auth_url": data_auth_url,
    }

    # ----------------------------
    # Task: build event JSON per table
    # ----------------------------
    @task
    def build_event_json_for_table(table: str) -> str:
        base_event: Dict[str, Any] = {
            "env_vars": env_vars_with_region,
            "table": table,
            "vendor": vendor,
        }
        base_event.update(exchange_extras)
        base_event.update(data_extras)
        return json.dumps(base_event)

    # ----------------------------
    # Task: latest framework zip
    # ----------------------------
    zip_prefix = f"s3://{bucket_name}/code/ETL/"
    latest_zip = get_latest_s3_uri.override(task_id="latest_framework_zip")(
        s3_prefix=zip_prefix,
        pattern="debi-etl-framework-glue*.zip",
    )

    # ----------------------------
    # Per-table tasks
    # ----------------------------
    for table in tables:
        safe = _safe_task_id(table)

        build_event = build_event_json_for_table.override(
            task_id=f"build_event_{safe}"
        )(table)

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

        # Latest parquet under the table prefix
        table_prefix = f"s3://{bucket_name}/data/{env}/{truncated_region}/{vendor}/{table}/"
        latest_parquet = get_latest_s3_uri.override(
            task_id=f"latest_parquet_{safe}"
        )(
            s3_prefix=table_prefix,
            pattern="*.parquet",
        )

        if copy_sql:
            params = generate_params(
                sql_params=sql_params,
                table=table,
                latest_s3_uri_task_id=f"latest_parquet_{safe}",
            )

            load_table = SQLExecuteQueryOperator(
                task_id=f"load_table_{safe}",
                conn_id="snowflake_salesforce",
                sql=copy_sql,
                params=params,
            )

            # Dependencies:
            latest_zip >> build_event >> run_glue
            latest_parquet >> load_table
            run_glue >> load_table
        else:
            latest_zip >> build_event >> run_glue


dag = salesforce_ingester_dag()
