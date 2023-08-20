

"""Get data from OECD API"""
from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.providers.slack.operators.slack import SlackAPIPostOperator



from airflow.decorators import dag, task
import requests

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "OECD_get_data"


dag = DAG(
    DAG_ID,
    default_args={"retries": 1},
    tags=["simple"],
    start_date=datetime(2023, 8, 9),
    catchup=False,
)

dag.doc_md = __doc__


task_get_data_oecd = SimpleHttpOperator(
    task_id="get_data_oecd",
    method="GET",
    http_conn_id="oecd_conn_id",
    headers={"Content-Type": "application/json"},
    log_response= True,
    dag=dag,
)

def transform_query(**kwargs):
    ti = kwargs["ti"]
    response = ti.xcom_pull(task_ids='get_data_oecd')
    json_file = json.loads(response)

    return json_file

transform_task = PythonOperator(
   task_id="transform_query",
   provide_context=True,
   python_callable=transform_query,
   dag=dag,
)

task_get_data_oecd >> transform_task