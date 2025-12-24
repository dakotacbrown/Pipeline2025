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
from airflow.providers.snowflake.operators.snowflake import (
    SQLExecuteQueryOperator,
)


def _safe_task_id(s: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", str(s))
    return s.strip("_").lower()


@task(task_id="get_latest_s3_object")
def get_latest_s3_object(
    s3_prefix: str,
    filename_pattern: Optional[str] = None,
) -> str:
    """
    Return s3://bucket/key for newest object under prefix.
    Optionally filter by filename pattern (fnmatch).
    """
    u = urlparse(s3_prefix)
    if u.scheme != "s3":
        raise ValueError(f"Expected s3:// prefix, got {s3_prefix}")

    bucket = u.netloc
    prefix = u.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    matches = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key.rsplit("/", 1)[-1]
            if filename_pattern and not fnmatch.fnmatch(name, filename_pattern):
                continue
            matches.append(obj)

    if not matches:
        raise ValueError(f"No objects found under {s3_prefix}")

    newest = max(matches, key=lambda o: o["LastModified"])
    return f"s3://{bucket}/{newest['Key']}"


def generate_params(
    sql_params: Dict[str, Any],
    table: str,
    s3_uri: str,
) -> Dict[str, str]:
    return {
        "target_table": f"{sql_params.get('DATABASE','VALIDATION')}.{sql_params.get('SCHEMA','PUBLIC')}.{table}",
        "s3_uri": s3_uri,
    }


@dag(
    dag_id="debi_ingester_glue_runner",
    schedule=None,
    start_date=datetime(2023, 12, 23, tzinfo=pendulum.timezone("UTC")),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 5,
        "retry_delay": pendulum.duration(minutes=5),
    },
)
def salesforce_ingester_dag():

    workflow = Variable.get(
        "INGESTER_WORKFLOW_SALESFORCE",
        default_var={},
        deserialize_json=True,
    )

    tables: List[str] = workflow.get("INGESTER_TABLES", [])
    sql_params: Dict[str, Any] = workflow.get("INGESTER_SQL_PARAMS", {})
    copy_sql: str = Variable.get("INGESTER_COPY_SQL")

    bucket = workflow.get("BUCKET_NAME", "dummy-bucket")
    region = workflow.get("REGION", "us-east-1")
    vendor = "salesforce"

    # ---- Framework zip ----
    framework_prefix = f"s3://{bucket}/code/ETL/"
    latest_framework_zip = get_latest_s3_object.override(
        task_id="latest_framework_zip"
    )(
        framework_prefix,
        filename_pattern="*.zip",
    )

    for table in tables:
        safe = _safe_task_id(table)

        parquet_prefix = f"s3://{bucket}/data/{region}/{vendor}/{table}/"

        latest_parquet = get_latest_s3_object.override(
            task_id=f"latest_parquet_{safe}"
        )(parquet_prefix)

        params = generate_params(sql_params, table, latest_parquet)

        load = SQLExecuteQueryOperator(
            task_id=f"load_table_{safe}",
            conn_id="snowflake_salesforce",
            sql=copy_sql,
            params=params,
        )

        latest_framework_zip >> latest_parquet >> load


dag = salesforce_ingester_dag()
