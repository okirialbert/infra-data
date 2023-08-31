"""Get green growth regional data from OECD API"""
from __future__ import annotations

import os
from datetime import datetime
import logging
import pandas as pd

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from transform_oecd_operator import OECDTransformOperator

from airflow.decorators import dag



ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "OECD_get_gg_regional"


dag = DAG(
    DAG_ID,
    default_args={"retries": 1},
    tags=["simple"],
    start_date=datetime(2023, 8, 9),
    catchup=False,
)

dag.doc_md = __doc__

df_eco_op_path = "df_eco_op.csv"
df_env_dim_path = "df_env_dim.csv"
df_green_growth_path = "df_green_growth.csv"
df_headline_ind_path = "df_headline_ind.csv"
df_env_res_prod_path = "df_env_res_prod.csv"

# Loading Tasks

task_df_eco_op = SimpleHttpOperator(
    task_id="get_df_eco_op",
    method="GET",
    http_conn_id="df_eco_op",
    headers={"Accept": "application/vnd.sdmx.data+json;charset=utf-8;version=1.0"},
    log_response= True,
    dag=dag,
)

task_df_env_dim = SimpleHttpOperator(
    task_id="get_df_env_dim",
    method="GET",
    http_conn_id="df_env_dim",
    headers={"Accept": "application/vnd.sdmx.data+json;charset=utf-8;version=1.0"},
    log_response= True,
    dag=dag,
)

task_df_green_growth = SimpleHttpOperator(
    task_id="get_df_green_growth",
    method="GET",
    http_conn_id="df_green_growth",
    headers={"Accept": "application/vnd.sdmx.data+json;charset=utf-8;version=1.0"},
    log_response= True,
    dag=dag,
)

task_df_headline_ind = SimpleHttpOperator(
    task_id="get_df_headline_ind",
    method="GET",
    http_conn_id="df_headline_ind",
    headers={"Accept": "application/vnd.sdmx.data+json;charset=utf-8;version=1.0"},
    log_response= True,
    dag=dag,
)

task_df_env_res_prod = SimpleHttpOperator(
    task_id="get_df_env_res_prod",
    method="GET",
    http_conn_id="df_env_res_prod",
    headers={"Accept": "application/vnd.sdmx.data+json;charset=utf-8;version=1.0"},
    log_response= True,
    dag=dag,
)


# Transformation Tasks

transform_eco_task = OECDTransformOperator(
    task_id = "transform_df_eco_op",
    response = "{{task_instance.xcom_pull(task_ids='get_df_eco_op')}}",
    output_path = df_eco_op_path
)
 
transform_env_task = OECDTransformOperator(
    task_id = "transform_df_env_dim",
    response = "{{task_instance.xcom_pull(task_ids='get_df_env_dim')}}",
    output_path = df_env_dim_path
)

transform_gg_task = OECDTransformOperator(
    task_id = "transform_df_green_growth",
    response = "{{task_instance.xcom_pull(task_ids='get_df_green_growth')}}",
    output_path = df_green_growth_path
)

transform_headline_ind = OECDTransformOperator(
    task_id = "transform_df_headline_ind",
    response = "{{task_instance.xcom_pull(task_ids='get_df_headline_ind')}}",
    output_path = df_headline_ind_path
)

transform_env_res_prod = OECDTransformOperator(
    task_id = "transform_df_env_res_prod",
    response = "{{task_instance.xcom_pull(task_ids='get_df_env_res_prod')}}",
    output_path = df_env_res_prod_path
)



def read_store():
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    curr = conn.cursor()
    df_eco_op_path_abs = os.path.join(df_eco_op_path)
    df_env_dim_path_abs = os.path.join(df_env_dim_path)
    df_green_growth_path_abs = os.path.join(df_green_growth_path)
    df_env_res_prod_path_abs = os.path.join(df_env_res_prod_path)
    df_headline_ind_path_abs = os.path.join(df_headline_ind_path)

    # CSV loading to table.
    with open(df_eco_op_path_abs, 'r') as a:
        curr.copy_from(a, 'df_eco_op', sep=',', null="")
        conn.commit()
    logging.info("Loading values to df_env_dim")
    with open(df_env_dim_path_abs, 'r') as b:
        curr.copy_from(b, 'df_env_dim', sep=',', null="")
        conn.commit()
    logging.info("Loading values to df_green_growth")
    with open(df_green_growth_path_abs, 'r') as c:
        curr.copy_from(c, 'df_green_growth', sep=',', null="")
        conn.commit()
    logging.info("Loading values to df_env_res_prod")
    with open(df_env_res_prod_path_abs, 'r') as d:
        curr.copy_from(d, 'df_env_res_prod', sep=',', null="")
        conn.commit()
    logging.info("Loading values to df_headline_ind")
    with open(df_headline_ind_path_abs, 'r') as e:
        curr.copy_from(e, 'df_headline_ind', sep=',', null="")
        conn.commit()
       

create_table = PostgresOperator(
        task_id="create_oecd_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS df_eco_op (
            series NUMERIC,
            ref_area VARCHAR NOT NULL,
            frequency VARCHAR NOT NULL,
            subject VARCHAR NOT NULL,
            measure VARCHAR NOT NULL,
            unit_measure VARCHAR NOT NULL,
            time DATE NOT NULL
            );
            CREATE TABLE IF NOT EXISTS df_env_dim (
            series NUMERIC,
            ref_area VARCHAR NOT NULL,
            frequency VARCHAR NOT NULL,
            subject VARCHAR NOT NULL,
            measure VARCHAR NOT NULL,
            unit_measure VARCHAR NOT NULL,
            time DATE NOT NULL
            );
            CREATE TABLE IF NOT EXISTS df_green_growth (
            series NUMERIC,
            ref_area VARCHAR NOT NULL,
            frequency VARCHAR NOT NULL,
            subject VARCHAR NOT NULL,
            measure VARCHAR NOT NULL,
            unit_measure VARCHAR NOT NULL,
            time DATE NOT NULL
            );
            CREATE TABLE IF NOT EXISTS df_env_res_prod (
            series NUMERIC,
            ref_area VARCHAR NOT NULL,
            frequency VARCHAR NOT NULL,
            subject VARCHAR NOT NULL,
            measure VARCHAR NOT NULL,
            unit_measure VARCHAR NOT NULL,
            time DATE NOT NULL
            );
            CREATE TABLE IF NOT EXISTS df_headline_ind (
            series NUMERIC,
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


task_df_eco_op >> transform_eco_task
task_df_env_dim >> transform_env_task
task_df_green_growth >> transform_gg_task
task_df_env_res_prod >> transform_env_res_prod
task_df_headline_ind >> transform_headline_ind

[transform_eco_task, transform_env_task, transform_gg_task, transform_env_res_prod, transform_headline_ind] >> create_table >> read_store_task

# >> populate_table