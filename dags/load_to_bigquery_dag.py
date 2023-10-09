from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from bigquery import load_json_to_bigquery
from google.cloud import bigquery
import os
import time
import logging
from pathlib import Path


service_acc_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

client = bigquery.Client.from_service_account_json(service_acc_key_path)

DATASET_ID = 'mobileviking'
TABLE_NAMES = ['products', 'packs']

DEFAULT_DAG_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


def check_ndjson_exist(file_names):
    time.sleep(30)
    counter = 0
    while counter < 2:
        all_exist = True
        for file_name in file_names:
            file_path = Path.cwd() / 'data' / 'raw_data' / 'ndjson' / f'{file_name}.ndjson'
            if not file_path.is_file():
                all_exist = False
                break
        if all_exist:
            return True
        else:
            counter += 1
            time.sleep(5)


@dag(
    dag_id='load_to_bigquery_dag',
    start_date=datetime(2023, 10, 5),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_DAG_ARGS
)
def load_to_bigquery_dag():

    wait_for_file = PythonSensor(
        task_id='wait_for_file',
        python_callable=check_ndjson_exist,
        op_kwargs={"file_names": TABLE_NAMES},
        mode='reschedule',
        timeout=70,

    )

    load_to_bigquery = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_json_to_bigquery,
        op_kwargs={"client": client, "dataset_id": DATASET_ID, "table_names": TABLE_NAMES},

    )

    wait_for_file >> load_to_bigquery


load_job = load_to_bigquery_dag()
