from datetime import datetime

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dags.common.dag_utilities import failover_managed_dag_tag

doc_md_dag = """
salesforce_scheduler DAG triggers the salesforce DAG based on the list of Salesforce sources

This DAG receives the sources from the ingester variable and triggers the salesforce DAG for each of the sources.
successfully execution.
"""


@dag(
    default_args={"owner": "airflow"},
    tags=["airflow-2.x.x-compatible", failover_managed_dag_tag()],
    schedule="0 11,16,19 * * *",
    dag_id="debi_salesforce_glue_triggerer",
    max_active_runs=1,
    start_date=datetime(2024, 8, 12, tzinfo=pendulum.timezone("UTC")),
    catchup=False,
    doc_md=doc_md_dag,
)
def dag_scheduler():
    previous_task = None
    salesforce = TriggerDagRunOperator(
        task_id="trigger_salesforce_generic_dag",
        trigger_dag_id="debi_generic_ingester_glue_runner",
        wait_for_completion=True,
        conf={
            "vendor": "salesforce",
            # Salesforce credentials
            "credentials": {
                "username": Variable.get("C1S_SALESFORCE_USERNAME", None),
                "password": Variable.get("C1S_SALESFORCE_PASSWORD", None),
                "client_id": Variable.get("C1S_SALESFORCE_CLIENTID", None),
                "client_secret": Variable.get(
                    "C1S_SALESFORCE_CLIENTSECRET", None
                ),
            },
        },
    )

    if previous_task:
        previous_task >> salesforce
    previous_task = salesforce

    # trigger_transformations = TriggerDagRunOperator(
    #     task_id="trigger_salesforce_transformations",
    #     trigger_dag_id="salesforce_transformations",
    #     wait_for_completion=True,
    # )

    # if previous_task:
    #     previous_task >> trigger_transformations


dag_scheduler = dag_scheduler()
