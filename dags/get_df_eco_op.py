"""Get (DSD_GG@DF_ECO_OP) Green Growth - Economic opportunities and policy responses datasetz from OECD API"""
from __future__ import annotations

import os
from datetime import datetime
import pandas as pd
import json
import logging

from io import StringIO

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.decorators import dag



ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "get_df_eco_op"


dag = DAG(
    DAG_ID,
    default_args={"retries": 1},
    tags=["oecd"],
    start_date=datetime(2023, 8, 9),
    catchup=False,
)

dag.doc_md = __doc__


task_get_data_oecd = SimpleHttpOperator(
    task_id="get_data_oecd",
    method="GET",
    http_conn_id="oecd_conn_id",
    endpoint="OECD.ENV.EPI/DSD_GG@DF_ECO_OP/1.0/*.A.*.*.*?c[TIME_PERIOD]=ge:1981+le:2021&attributes=dsd&measures=all&updatedAfter=2015-01-01T00:00:00.000-01:00",
    headers={"Accept": "application/vnd.sdmx.data+csv;charset=utf-8;version=2"},
    log_response= True,
    dag=dag,
)


def transform_response(**kwargs):
    ti = kwargs["ti"]
    response = ti.xcom_pull(task_ids='get_data_oecd')

    file_data = str(response)
    df_data = pd.read_csv(StringIO(file_data), sep=',', lineterminator='\n')

    obj_columns = ['STRUCTURE','STRUCTURE_ID','ACTION','REF_AREA','FREQ','MEASURE','UNIT_MEASURE','ACTIVITY','OBS_STATUS','OBS_STATUS_2','PRICE_BASE']
    df_data[obj_columns] = df_data[obj_columns].astype("string")
    df_data['TIME_PERIOD']= pd.to_datetime(df_data['TIME_PERIOD'], format='%Y')

    df_columns = ['STRUCTURE', 'STRUCTURE_ID', 'ACTION', 'REF_AREA', 'FREQ', 'MEASURE',
       'UNIT_MEASURE', 'ACTIVITY', 'TIME_PERIOD', 'OBS_VALUE', 'OBS_STATUS',
       'OBS_STATUS_2', 'UNIT_MULT', 'PRICE_BASE', 'BASE_PER', 'TIMELINESS']
    
    df_data = df_data[df_columns]

    tmp_path = os.path.join('obs.csv')
    df_data.to_csv(tmp_path, header=None, index=False)

    ti.xcom_push(key="store_path", value=tmp_path)


def read_store(**kwargs):

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    curr = conn.cursor()

    ti = kwargs["ti"]
    csv_path = os.path.join('obs.csv')

    # CSV loading to table.
    with open(csv_path, 'r') as a:
        curr.copy_from(a, 'gg_eco_op', sep=',', null="", columns=('structure', 'structure_id', 'action', 'ref', 'freq', 'measure',
       'unit_measure', 'activity', 'time_period', 'obs_value', 'obs_status',
       'obs_status_2', 'unit_mult', 'price_base', 'base_per', 'timeliness'))
        conn.commit()


transform_response_task = PythonOperator(
   task_id="transform_response",
   provide_context=True,
   python_callable=transform_response,
   dag=dag,
)

read_store_task = PythonOperator(
   task_id="read_store",
   provide_context=True,
   python_callable=read_store,
   dag=dag,
)


create_table = PostgresOperator(
        task_id="create_oecd_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS gg_eco_op (
            obs_id SERIAL PRIMARY KEY,
            structure VARCHAR,
            structure_id VARCHAR,
            action VARCHAR,
            ref VARCHAR,
            freq VARCHAR,
            measure VARCHAR,
            unit_measure VARCHAR,
            activity VARCHAR,
            time_period DATE,
            obs_value NUMERIC,
            obs_status VARCHAR,
            obs_status_2 VARCHAR,
            unit_mult NUMERIC,
            price_base VARCHAR,
            base_per NUMERIC,
            timeliness NUMERIC
            );
            """)


signal_task = BashOperator(
    task_id="signal_op",
    bash_command='echo "Signal Complete";'
)

task_get_data_oecd >> transform_response_task >> create_table >> read_store_task >> signal_task