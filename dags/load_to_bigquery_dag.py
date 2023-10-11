from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.time_delta import TimeDeltaSensor


from bigquery2 import load_to_bq
import google.cloud.bigquery as bq

import os

from utils import check_file_exist


service_acc_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

client = bq.Client.from_service_account_json(service_acc_key_path)


PROJECT_ID = 'arched-media-273319'
DATASET_ID = 'test123'
TABLE_NAMES = ['competitors', 'products', 'features', 'prices', 'packs']
COMPETITORS = ['mobileviking', 'scarlet']
FILE_NAMES = ['products', 'packs']

BQ_TABLE_SCHEMAS = {
    # this table should contain immutable data
    "competitors": [
        bq.SchemaField('competitor_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('created_at', 'DATETIME', mode='REQUIRED'),

    ],

    "products": [
        bq.SchemaField('product_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('competitor_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),

    ],
    "features": [
        bq.SchemaField('feature_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_category', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_url', 'STRING', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),
        bq.SchemaField('data', 'FLOAT', mode='NULLABLE'),
        bq.SchemaField('minutes', 'FLOAT', mode='NULLABLE'),
        bq.SchemaField('sms', 'INTEGER', mode='NULLABLE'),
        bq.SchemaField('upload_speed', 'FLOAT', mode='NULLABLE'),
        bq.SchemaField('download_speed', 'FLOAT', mode='NULLABLE'),

    ],
    "prices": [
        bq.SchemaField('price_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('feature_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('price', 'FLOAT', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),
    ],
    "packs": [
        bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('pack_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('pack_url', 'STRING', mode='REQUIRED'),
        bq.SchemaField('price', 'FLOAT', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),
        bq.SchemaField('mobile_product_name', 'STRING', mode='NULLABLE'),
        bq.SchemaField('internet_product_name', 'STRING', mode='NULLABLE'),
    ]

}

DEFAULT_DAG_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}



@dag(
    dag_id='load_to_bigquery_dag',
    start_date=datetime(2023, 10, 5),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_DAG_ARGS
)
def load_to_bigquery_dag():


    delay_task = TimeDeltaSensor(
        task_id='delay_task',
        delta=timedelta(seconds=80)
    )

    wait_for_file = PythonSensor(
        task_id='wait_for_file',
        python_callable=check_file_exist,
        op_kwargs={"dir": "cleaned_data","competitors": COMPETITORS, "file_names": FILE_NAMES, "file_type": "ndjson"},
        mode='reschedule',
        timeout=180,

    )

    load_to_bigquery = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bq,
        op_kwargs= {
            "client": client,
            "project_id": PROJECT_ID,
            "dataset_id": DATASET_ID,
            "table_names": TABLE_NAMES,
            "table_schemas": BQ_TABLE_SCHEMAS
            }

    )

    delay_task >> wait_for_file >> load_to_bigquery


load_job = load_to_bigquery_dag()
