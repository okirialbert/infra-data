"""Get data from OECD API"""
from __future__ import annotations

import os
from datetime import datetime
import pandas as pd
import re
import json
import tempfile
import logging

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from airflow.decorators import dag



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
        obs = obs.reset_index()
        obs = obs[['series', 'ref_area', 'frequency', 'subject', 'measure', 'unit_measure', 'time']]
        obs = obs.astype({'series': 'float32', 'ref_area': 'string', 'frequency': 'string', 'subject': 'string', 'measure': 'string', 'unit_measure': 'string'})
        obs['time']= pd.to_datetime(obs['time'], format='%Y')

        tmp_path = os.path.join("obs.csv")
        obs.to_csv(tmp_path ,header=None, index=False)

        ti.xcom_push(key="store_path", value=tmp_path)

            # data = obs.pivot_table(index = ['ref_area', 'unit_measure'], columns = ['time'], values = 'series')
            
        return obs
    
def read_store(**kwargs):
    ti = kwargs["ti"]
    path = ti.xcom_pull(key="store_path", task_ids='transform_response_to_df')
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    curr = conn.cursor()
    # CSV loading to table.
    logging.info("Loading values to oecd_table")
    with open(path, 'r') as f:
        curr.copy_from(f, 'oecd_table', sep=',')
        conn.commit()
       

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
            CREATE TABLE IF NOT EXISTS oecd_table (
            series NUMERIC NOT NULL,
            ref_area VARCHAR NOT NULL,
            frequency VARCHAR NOT NULL,
            subject VARCHAR NOT NULL,
            measure VARCHAR NOT NULL,
            unit_measure VARCHAR NOT NULL,
            time DATE NOT NULL
            );
          """,
    )


read_store_task = PythonOperator(
   task_id="read_store",
   provide_context=True,
   python_callable=read_store,
   dag=dag,
)



# populate_table = PostgresOperator(
#         task_id="populate_table",
#         # sql="""
#         #     INSERT INTO green_table ( series, ref_area, frequency, subject, measure, unit_measure, time )
#         #     VALUES
#         #     ({{task_instance.xcom_pull(task_ids='transform_response_to_df')}});
#         #     """,
#         sql = '''
#         COPY green_table
#         FROM '{{task_instance.xcom_pull(key='store_path', task_ids='transform_response_to_df')}}' 
#         DELIMITER ',' CSV;
#         '''
#     )

task_get_data_oecd >> transform_task >> create_table >> read_store_task 

# >> populate_table