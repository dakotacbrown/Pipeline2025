from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from dags.common.dag_utilities import (
    get_shairflow_environment,
    get_shairflow_region,
)


def _safe_task_id(s: str) -> str:
    # Airflow task_ids: letters/numbers/_ only
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", str(s))
    return s.strip("_").lower()


# Parse-time config (Variables are read when the DAG is parsed)
env = get_shairflow_environment().lower()
region = get_shairflow_region().lower()

etl_job_name = Variable.get("INGESTER_GLUE_JOB_NAME", "etl-job")
etl_conn_name = Variable.get("INGESTER_GLUE_CONN_NAME", "etl-net-conn")
run_mode = Variable.get("INGESTER_RUN_MODE", "qa")

tables: List[str] = json.loads(Variable.get("INGESTER_TABLES", "[]"))
vendor = Variable.get("INGESTER_VENDOR", "salesforce")
config_path = Variable.get("INGESTER_CONFIG_PATH", None)
repo_name = Variable.get("INGESTER_CONFIG_REPO_NAME", "config_management")
github_token = Variable.get("CISCOREDATASERVICES_GITHUB_PASSWORD", None)

start_date = Variable.get("INGESTER_START_DATE", "2000-01-01")
end_date = Variable.get(
    "INGESTER_END_DATE", datetime.now().strftime("%Y-%m-%d")
)

# These are dict-shaped in your code (you use .get and **expansion), so default to "{}"
env_vars: Dict[str, Dict[str, str]] = json.loads(
    Variable.get("INGESTER_ENV_VARS", "{}")
)
exchange_extras: Dict[str, Any] = json.loads(
    Variable.get("INGESTER_EXCHANGE_EXTRAS", "{}")
)
data_extras: Dict[str, Any] = json.loads(
    Variable.get("INGESTER_DATA_EXTRAS", "{}")
)


with DAG(
    dag_id="debi_ingestor_glue_runner",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["glue", "ingestion"],
) as dag:

    @task(task_id="build_event")
    def build_event_json() -> str:
        """
        Build the '--event' payload your run_step.py expects.
        You said you want to build the event in the DAG (not from dag_run.conf).
        """
        base_event: Dict[str, Any] = {
            "env_vars": (
                env_vars.get(env, {}) if isinstance(env_vars, dict) else {}
            )
        }

        if isinstance(exchange_extras, dict):
            base_event.update(exchange_extras)

        if isinstance(data_extras, dict) and data_extras:
            base_event.update(data_extras)

        return json.dumps(base_event)

    event_json = build_event_json()

    if not isinstance(tables, list):
        tables = [tables]

    for table in tables:
        task_id = f"run_glue_job__{_safe_task_id(table)}"

        run_glue = GlueJobOperator(
            task_id=task_id,
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
                # Pull the JSON string built by build_event
                "--event": "{{ ti.xcom_pull(task_ids='build_event') }}",
            },
            wait_for_completion=True,
        )

        event_json >> run_glue
