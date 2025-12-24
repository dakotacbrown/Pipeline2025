from __future__ import annotations

import ast
import fnmatch
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
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


def _safe_task_id(s: str) -> str:
    """Airflow task_ids: letters/numbers/_ only."""
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", str(s))
    return s.strip("_").lower()


def _safe_json_loads(raw: Optional[str], default: Any) -> Any:
    """
    Parse JSON safely.
    - Primary: json.loads
    - Fallback: ast.literal_eval for "python literal" strings (single quotes)
    """
    if raw is None or raw == "":
        return default

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(raw)
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Invalid JSON/python literal: {raw!r}") from e


def _split_s3_uri(s3_uri: str) -> Tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {s3_uri}")
    no_scheme = s3_uri[len("s3://") :]
    bucket, _, key = no_scheme.partition("/")
    return bucket, key


@task(task_id="get_latest_framework_zip")
def get_latest_framework_zip(
    s3_prefix: str,
    pattern: str = "debi-etl-framework-glue*.zip",
) -> str:
    """
    Find newest zip matching pattern under s3_prefix.
    Example: s3://<bucket>/code/ETL/
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
            if fnmatch.fnmatch(filename, pattern):
                matches.append(obj)

    if not matches:
        raise ValueError(f"No files matched {pattern!r} under {s3_prefix}")

    newest = max(matches, key=lambda o: o["LastModified"])
    return f"s3://{bucket}/{newest['Key']}"


# -------------------------
# Parse-time config
# -------------------------
env = get_shairflow_environment().lower()
region = get_shairflow_region().lower()
truncated_region = get_truncated_shairflow_region()
bucket_name = get_bucket_name(env, truncated_region)
c1_oauth_url = get_c1s_oauth_endpoint(env)

etl_job_name = Variable.get("INGESTER_GLUE_JOB_NAME", "etl-job")
etl_conn_name = Variable.get("INGESTER_GLUE_CONN_NAME", "etl-net-conn")
run_mode = Variable.get("INGESTER_RUN_MODE", "once")

tables_raw = Variable.get("INGESTER_TABLES", "[]")
tables_any = _safe_json_loads(tables_raw, [])
tables: List[str] = (
    tables_any if isinstance(tables_any, list) else [str(tables_any)]
)

vendor = Variable.get("INGESTER_VENDOR", "salesforce")
config_path = Variable.get("INGESTER_CONFIG_PATH", "") or ""
repo_name = Variable.get("INGESTER_CONFIG_REPO_NAME", "config_management")

# Ensure we never pass None into Glue args (Glue/botocore will reject None)
github_token = Variable.get("C1SCOREDATASERVICES_GITHUB_PASSWORD", "") or ""
start_date = Variable.get("INGESTER_START_DATE", "2000-01-01")
end_date = Variable.get(
    "INGESTER_END_DATE", datetime.now().strftime("%Y-%m-%d")
)

c1scoredataservices_exchange_id = (
    Variable.get("C1SCOREDATASERVICES_EXCHANGE_ID", "") or ""
)
c1scoredataservices_exchange_secret = (
    Variable.get("C1SCOREDATASERVICES_EXCHANGE_SECRET", "") or ""
)

username = Variable.get("C1S_SALESFORCE_USERNAME", "") or ""
password = Variable.get("C1S_SALESFORCE_PASSWORD", "") or ""
client_id = Variable.get("C1S_SALESFORCE_CLIENTID", "") or ""
client_secret = Variable.get("C1S_SALESFORCE_CLIENTSECRET", "") or ""

env_vars_raw = Variable.get("INGESTER_ENV_VARS", "{}")
env_vars_any = _safe_json_loads(env_vars_raw, {})
env_vars: Dict[str, Dict[str, str]] = (
    env_vars_any if isinstance(env_vars_any, dict) else {}
)
data_auth_url = (
    "https://partner-apis-it.cloud.capitalone.com/third-party/salesforce/services/oauth2/token"
    if env == "qa"
    else "https://partner-apis.cloud.capitalone.com/third-party/salesforce/services/oauth2/token"
)

env_vars_with_region: Dict[str, Dict[str, str]] = {
    k: {
        **(v if isinstance(v, dict) else {}),
        "AWS_REGION": region,
        "BUCKET_NAME": bucket_name,
    }
    for k, v in env_vars.items()
}

exchange_extras: Dict[str, Any] = {
    "c1_oauth_url": c1_oauth_url,
    "exchange_headers": {"Content-Type": "application/x-www-form-urlencoded"},
    "exchange_data": {
        "client_id": c1scoredataservices_exchange_id,
        "client_secret": c1scoredataservices_exchange_secret,
        "grant_type": "client_credentials",
    },
}

data_extras: Dict[str, Any] = {
    "data_headers": {
        "Content-Type": "application/json",
        "Accept": "application/json; v=1",
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

doc_md_dag = """
Salesforce Ingester Glue DAG orchestrates the following
1. Collection of raw data
2. Storage of raw data to S3
"""


@dag(
    tags=[
        "invoke-lambda",
        "airflow-2.x.x-compatible",
        failover_managed_dag_tag(),
    ],
    default_args={
        "owner": "Airflow",
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
    doc_md=doc_md_dag,
)
def salesforce_ingester_dag():
    @task
    def build_event_json_for_table(table: str) -> str:
        base_event: Dict[str, Any] = {
            "env_vars": env_vars_with_region.get(env, {}),
            "table": table,
            "vendor": vendor,
        }
        base_event.update(exchange_extras)
        base_event.update(data_extras)
        return json.dumps(base_event)

    # Single known prefix from you:
    zip_prefix = f"s3://{bucket_name}/code/ETL/"
    latest_zip = get_latest_framework_zip(zip_prefix)

    for table in tables:
        safe = _safe_task_id(table)

        build_event = build_event_json_for_table.override(
            task_id=f"build_event_{safe}"
        )(table)

        run_glue = GlueJobOperator(
            task_id=f"run_glue_job__{safe}",
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
                # ONE zip only:
                "--extra-py-files": "{{ ti.xcom_pull(task_ids='get_latest_framework_zip') }}",
                "--event": f"{{{{ ti.xcom_pull(task_ids='build_event_{safe}') | tojson }}}}",
            },
            wait_for_completion=True,
        )

        latest_zip >> build_event >> run_glue


dag = salesforce_ingester_dag()
