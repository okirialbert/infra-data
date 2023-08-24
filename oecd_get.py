"""Get data from OECD API"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from airflow.decorators import dag

import pandas as pd
import re
import json

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "OECD_get_store"


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
    headers={"Accept": "application/vnd.sdmx.data+json;charset=utf-8;version=1.0"},
    log_response= True,
    dag=dag,
)


def transform_response(**kwargs):
    # Data transformation

    ti = kwargs["ti"]
    response = ti.xcom_pull(task_ids='get_data_oecd')


    responseJson = json.loads(response)
    dataJson = responseJson.get('data')
    metaJson = responseJson.get('meta')
    ti.xcom_push(key="response_meta", value=metaJson)

    obsList = dataJson.get('dataSets')[0].get('observations')

    if (len(obsList) > 0):

        timeList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'TIME_PERIOD'][0]['values']
        refArea = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'REF_AREA'][0]['values']
        freqList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'FREQ'][0]['values']
        subjectList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'ACTIVITY'][0]['values']
        measureList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'MEASURE'][0]['values']
        unitMeasureList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'UNIT_MEASURE'][0]['values']

        obs = pd.DataFrame(obsList).transpose()
        obs.rename(columns = {0: 'series'}, inplace = True)
        obs['id'] = obs.index
        obs = obs[['id', 'series']]
        obs['dimensions'] = obs.apply(lambda x: re.findall('\d+', x['id']), axis = 1)
        obs['ref_area'] = obs.apply(lambda x: refArea[int(x['dimensions'][0])]['id'], axis = 1)
        obs['frequency'] = obs.apply(lambda x: freqList[int(x['dimensions'][1])]['id'], axis = 1)
        obs['subject'] = obs.apply(lambda x: subjectList[int(x['dimensions'][4])]['id'], axis = 1)
        obs['measure'] = obs.apply(lambda x: measureList[int(x['dimensions'][2])]['id'], axis = 1)
        obs['unit_measure'] = obs.apply(lambda x: unitMeasureList[int(x['dimensions'][3])]['id'], axis = 1)
        obs['time'] = obs.apply(lambda x: timeList[int(x['dimensions'][5])]['id'], axis = 1)
        obs['names'] = obs['subject'] + '_' + obs['measure']

            # data = obs.pivot_table(index = ['ref_area', 'unit_measure'], columns = ['time'], values = 'series')
            
        return (obs)

transform_task = PythonOperator(
   task_id="transform_response_to_df",
   provide_context=True,
   python_callable=transform_response,
   dag=dag,
)

create_table = PostgresOperator(
        task_id="create_oecd_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS green (
            obs_id SERIAL PRIMARY KEY,
            series NUMERIC NOT NULL,
            ref_area VARCHAR NOT NULL,
            time DATE NOT NULL,
            frequency VARCHAR NOT NULL
            measure VARCHAR NOT NULL
            unit_measure VARCHAR NOT NULL
            subject VARCHAR NOT NULL);
          """,
    )

task_get_data_oecd >> transform_task >> create_table