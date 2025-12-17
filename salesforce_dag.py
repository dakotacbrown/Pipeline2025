from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator


def build_event_json(**context) -> str:
    """
    Build the `--event` payload your run_step.py expects.
    You can pass overrides at trigger time via dag_run.conf.
    """
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}

    # Example shape based on your wrapper screenshots:
    # event.env_vars[env] -> dict of env vars to export
    env = conf.get("env", "dev")
    event = {
        "env_vars": {env: conf.get("env_vars", {"FOO": "bar"})},
        # OAuth bits (only include if your wrapper expects them)
        "c1_oauth_url": conf.get("c1_oauth_url", "https://example/token"),
        "exchange_headers": conf.get("exchange_headers", {}),
        "exchange_data": conf.get("exchange_data", {}),
        # Optional second token flow
        "data_headers": conf.get("data_headers", {}),
        "data_auth": conf.get("data_auth", {}),
        "data_auth_url": conf.get("data_auth_url", ""),
    }

    return json.dumps(event)


with DAG(
    dag_id="debi_ingestor_glue_runner",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["glue", "ingestion"],
) as dag:

    build_event = PythonOperator(
        task_id="build_event",
        python_callable=build_event_json,
    )

    run_glue = AwsGlueJobOperator(
        task_id="run_glue_job",
        job_name=Variable.get("DEBI_INGESTOR_GLUE_JOB_NAME"),  # or hardcode
        aws_conn_id="aws_default",
        region_name=Variable.get("AWS_REGION", default_var="us-east-1"),
        # If your Glue job is already created and points at run_step.py,
        # you generally do NOT need to set script_location here.
        # These become the args your run_step.py parses (argparse).
        script_args={
            "--env": "{{ dag_run.conf.get('env', 'dev') }}",
            "--run_mode": "{{ dag_run.conf.get('run_mode', 'once') }}",
            "--table": "{{ dag_run.conf.get('table', 'events_api') }}",
            "--vendor": "{{ dag_run.conf.get('vendor', 'default') }}",
            # GitHub config fetch (runner uses GithubConnection)
            "--repo_name": "{{ dag_run.conf.get('repo_name', 'my-repo') }}",
            "--file_path": "{{ dag_run.conf.get('file_path', 'path/to/config.yml') }}",
            "--github_token": Variable.get("GITHUB_TOKEN"),
            # Backfill dates (runner supports these)
            "--start_date": "{{ dag_run.conf.get('start_date', '') }}",
            "--end_date": "{{ dag_run.conf.get('end_date', '') }}",
            # Your runner supports repeatable `--extra_env KEY=VALUE`
            # (Glue args typically don’t love repeated keys; a common workaround is
            # to pass one combined string and split inside the runner, but if
            # yours already supports repeats and you can provide them, great.)
            # Example single value:
            "--extra_env": "{{ dag_run.conf.get('extra_env', '') }}",
            # Pass event as a JSON string
            "--event": "{{ ti.xcom_pull(task_ids='build_event') }}",
        },
        wait_for_completion=True,
        # You can also set `poll_interval=...` if you want.
    )

    build_event >> run_glue
