from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from bigquery import load_json_to_bigquery
from google.cloud import bigquery
import os

service_acc_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

client = bigquery.Client.from_service_account_json(service_acc_key_path)

DATASET_ID = 'mobileviking'
TABLE_NAMES = ['products', 'packs']

DEFAULT_DAG_ARGS = {
    'owner': 'admin',
    'retry': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(

    default_args=DEFAULT_DAG_ARGS,
    dag_id='load_to_bigquery_dag',
    start_date=datetime(2023, 10, 5),
    schedule_interval=None,
    catchup=False,


) as dag:

    load_to_bigquery = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_json_to_bigquery,
        op_kwargs={"client": client, "dataset_id": DATASET_ID, "table_names": TABLE_NAMES},

    )

    load_to_bigquery
