"""Get data from OECD table"""
from __future__ import annotations

import os
from datetime import datetime
import pandas as pd
import re
import json
import tempfile
from pathlib import Path

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from airflow.decorators import dag



ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "OECD_retrieve"


dag = DAG(
    DAG_ID,
    default_args={"retries": 1},
    tags=["simple"],
    start_date=datetime(2023, 8, 9),
    catchup=False,
)

dag.doc_md = __doc__


get_all_obs = PostgresOperator(
    task_id="get_all_observations",
    postgres_conn_id="postgres_default",
    sql="SELECT * FROM oecd_table WHERE ref_area = 'KEN';"
)

def read_values(**kwargs):
    ti = kwargs["ti"]
    table = ti.xcom_pull(task_ids='get_all_observations')
    
    return table

read_table = PythonOperator(
   task_id="read_table_values",
   provide_context=True,
   python_callable=read_values,
   dag=dag,
)

get_all_obs >> read_table