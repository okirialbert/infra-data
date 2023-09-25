from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
    BigQueryCreateExternalTableOperator,
)

from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.models.dag import DAG
from airflow.utils.dag_parsing_context import get_parsing_context
# from airflow.decorators import dag, task
from datetime import datetime
import os
import yaml
from io import StringIO
import json



# Import configuration details
my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "config.yaml")
with open(configuration_file_path) as yaml_file:
    configuration = yaml.safe_load(yaml_file)

current_dag_id = get_parsing_context().dag_id

for config_name, config in configuration.items():
    dag_id = f"dynamic_gcs_bq_dag_{config_name}"
    db_name = config_name
    tmp_path = os.path.join(f'{config_name}.csv')
    DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{dag_id}"
    DATA_SAMPLE_GCS_OBJECT_NAME = f"bigquery/{config_name}.csv"
    dataset_name = "oecd_dataset_test"
    DATASET = "oecd_dataset_test"
    PROJECT_ID = "infradata"
    EXT = "oecd_test_table"
    
    
    if current_dag_id is not None and current_dag_id != dag_id:
        continue  # skip generation of non-selected DAG

    dag =  DAG(dag_id=dag_id, 
               default_args={"retries": 1},
               tags=["oecd"],
               start_date=datetime(2023,  8, 9), 
               catchup=False,
               user_defined_macros={"PROJECT_ID": PROJECT_ID,"dataset_name": dataset_name, "EXT": EXT})
    with dag:
        task_get_data_oecd = SimpleHttpOperator(
            task_id="get_data_oecd",
            method="GET",
            http_conn_id="oecd_conn_id",
            endpoint=f"{config['url']}",
            headers={"Accept": "application/vnd.sdmx.data+csv;charset=utf-8;version=2"},
            dag=dag,
            )
        
        def transform_response(**kwargs):
            import pandas as pd
            ti = kwargs["ti"]
            response = ti.xcom_pull(task_ids='get_data_oecd')

            file_data = str(response)
            df_data = pd.read_csv(StringIO(file_data), sep=',', lineterminator='\n')

            obj_cols = list(df_data.select_dtypes(['object']).columns)
            df_data[obj_cols] = df_data[obj_cols].astype("string")

            df_data['TIME_PERIOD']= pd.to_datetime(df_data['TIME_PERIOD'], format='%Y')
            df_data = df_data.drop(columns=["DECIMALS\r"])
            cols_tuple = tuple(df_data.columns)
            
            
            df_data.to_csv(tmp_path, header=None, index=False)

            ti.xcom_push(key="tmp_path", value=tmp_path)

            # Create query
            e = '{"schema_details":[]}'
            so = json.loads(e)
            for col in df_data.columns:
                # s2 = col
                if str(df_data[col].dtypes) == 'string':
                    # s2 = col + " VARCHAR"
                    s2 =  {"name": col, "type": "STRING", "mode": "NULLABLE"}
                elif str(df_data[col].dtypes) == "float64":
                    # s2 = col + " NUMERIC"
                    s2 = {"name": col, "type": "FLOAT64", "mode": "NULLABLE"}
                elif str(df_data[col].dtypes) == "int64":
                    # s2 = col + " NUMERIC"
                    s2 = {"name": col, "type": "INT64", "mode": "NULLABLE"}
                elif str(df_data[col].dtypes) == "datetime64[ns]":
                    # s2 = col + " DATE"
                    s2 = {"name": col, "type": "TIME", "mode": "NULLABLE"}
                else:
                    print("There was a datatype not identified")
                
                so["schema_details"].append(s2)
                st = json.dumps(so["schema_details"])
            ti.xcom_push(key="schema_", value=st)

        transform_response_task = PythonOperator(
            task_id="transform_response",
            python_callable=transform_response,
            dag=dag
            )
        
        create_bucket = GCSCreateBucketOperator(
            task_id="create_bucket", 
            bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
            project_id=PROJECT_ID,
            dag=dag)

        create_dataset_task = BigQueryCreateEmptyDatasetOperator(
            task_id="create_dataset",
            dataset_id=dataset_name,
            project_id=PROJECT_ID,
            if_exists="ignore",
            dag=dag
        )

        upload_file = LocalFilesystemToGCSOperator(
            task_id="upload_file_to_bucket",
            src=tmp_path,
            dst=DATA_SAMPLE_GCS_OBJECT_NAME,
            bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
            dag=dag
        )
        

        
        create_external_table = BigQueryCreateExternalTableOperator(
            task_id="create_external_table",
            table_resource={
                "tableReference":{
                    "projectId":f"{PROJECT_ID}",
                    "datasetId":f"{DATASET}",
                    "tableId":f"{EXT}"},
                "externalDataConfiguration":{
                    "autodetect": True,
                    "sourceFormat": "CSV",
                    "sourceUris": [f'gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/{DATA_SAMPLE_GCS_OBJECT_NAME}']


                },},
            bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
            source_objects=[DATA_SAMPLE_GCS_OBJECT_NAME],
            
            )

        task_get_data_oecd >> transform_response_task >> create_bucket >> create_dataset_task >> upload_file >> create_external_table