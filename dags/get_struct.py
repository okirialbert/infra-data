"""Get structure data - OECD API"""
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
DAG_ID = "get_structure_data"


dag = DAG(
    DAG_ID,
    default_args={"retries": 1},
    tags=["oecd"],
    start_date=datetime(2023, 8, 9),
    catchup=False,
)

dag.doc_md = __doc__

task_get_data_structure_oecd = SimpleHttpOperator(
    task_id="get_data_structure_oecd",
    method="GET",
    http_conn_id="oecd_conn_structure_id",
    endpoint="OECD.ENV.EPI/DSD_GG@DF_ECO_OP/1.0?references=all",
    headers={"Accept": "application/vnd.sdmx.structure+json;charset=utf-8;version=1.0"},
    log_response= True,
    dag=dag,
)

def transform_structure_store(**kwargs):
    ti = kwargs["ti"]
    response = ti.xcom_pull(task_ids='get_data_structure_oecd')

    responseJson = json.loads(response)
    codelistsJson = responseJson.get('data').get('codelists')

    idList = [id['id'] for item in codelistsJson for id in item['codes']]
    nameList = [id['name'] for item in codelistsJson for id in item['codes']]

    catIdList = [item['id'] for item in codelistsJson for id in item['codes']]
    catNameList = [item['name'] for item in codelistsJson for id in item['codes']]

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    curr = conn.cursor()
    
    tempdf = pd.DataFrame(idList)
    tempdf.rename(columns = {0: 'id'}, inplace = True)
    tempdf['name'] = nameList
    tempdf['category_id'] = catIdList
    tempdf['category_name'] = catNameList
        
    columns = ['id', 'name', 'category_id', 'category_name']
        
    tempdf = tempdf[columns].astype("string")

    incList = tempdf.values.tolist()

    args = ','.join(curr.mogrify("(%s,%s,%s,%s)", i).decode('utf-8')
                for i in incList)
        
    curr.execute("INSERT INTO eco_op_meta VALUES " + (args))
    conn.commit()

transform_structure_task = PythonOperator(
   task_id="transform_structure",
   provide_context=True,
   python_callable=transform_structure_store,
   dag=dag,
)


create_meta_table = PostgresOperator(
        task_id="create_oecd_struct_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS eco_op_meta (
            id VARCHAR,
            name VARCHAR,
            category_id VARCHAR,
            category_name VARCHAR
            );
            """)

task_get_data_structure_oecd >> create_meta_table >> transform_structure_task