from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.empty import EmptyOperator

from bigquery import *
import google.cloud.bigquery as bq

import os

# Retrieve the Google Cloud service account key path from environment variables
service_acc_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# Initialize the bigquery client with the service account key
client = bq.Client.from_service_account_json(service_acc_key_path)


PROJECT_ID = 'arched-media-273319'
DATASET_ID = 'competitors_dataset'
TABLE_NAMES = ['competitors', 'products', 'features', 'product_prices', 'packs', 'logs']
COMPETITORS = ['mobileviking', 'scarlet']
FILE_NAMES = ['products', 'packs', 'logs']

BQ_TABLE_SCHEMAS = {
    "competitors": [
        bq.SchemaField('competitor_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('created_at', 'DATETIME', mode='REQUIRED'),

    ],

    "products": [
        bq.SchemaField('product_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_category', 'STRING', mode='REQUIRED'),
        bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('competitor_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('feature_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),

    ],
    "features": [
        bq.SchemaField('feature_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('product_url', 'STRING', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),
        bq.SchemaField('data', 'FLOAT', mode='NULLABLE'),
        bq.SchemaField('minutes', 'FLOAT', mode='NULLABLE'),
        bq.SchemaField('sms', 'INTEGER', mode='NULLABLE'),
        bq.SchemaField('upload_speed', 'FLOAT', mode='NULLABLE'),
        bq.SchemaField('download_speed', 'FLOAT', mode='NULLABLE'),

    ],
    "product_prices": [
        bq.SchemaField('price_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('feature_uuid', 'STRING', mode='REQUIRED'),
        bq.SchemaField('price', 'FLOAT', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),
    ],
    "packs": [
        bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('pack_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('pack_url', 'STRING', mode='REQUIRED'),
        bq.SchemaField('pack_description', 'STRING', mode='NULLABLE'),
        bq.SchemaField('price', 'FLOAT', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),
        bq.SchemaField('mobile_product_name', 'STRING', mode='NULLABLE'),
        bq.SchemaField('internet_product_name', 'STRING', mode='NULLABLE'),
    ],
    "logs": [
        bq.SchemaField('competitor_name', 'STRING', mode='REQUIRED'),
        bq.SchemaField('scraped_at', 'DATETIME', mode='REQUIRED'),
        bq.SchemaField('error_details', 'STRING', mode='NULLABLE'),
        bq.SchemaField('status', 'STRING', mode='NULLABLE'),
    ]

}

DEFAULT_DAG_ARGS = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='load_to_bigquery_dag',
    start_date=datetime(2023, 10, 5),
    schedule_interval=None,
    description='DAG to load data into BigQuery. It handles creation of datasets and tables and loads data into BigQuery.',
    catchup=False,
    default_args=DEFAULT_DAG_ARGS
)
def load_to_bigquery_dag():

    delay_task = TimeDeltaSensor(
        task_id='delay_task',
        delta=timedelta(seconds=2)
    )

    create_dataset = PythonOperator(
        task_id='create_dataset',
        python_callable=create_dataset_if_not_exist,
        op_kwargs={
            "client": client,
            "project_id": PROJECT_ID,
            "dataset_id": DATASET_ID,
        },
    )

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table_if_not_exist,
        op_kwargs={
            "client": client,
            "project_id": PROJECT_ID,
            "dataset_id": DATASET_ID,
            "tables": TABLE_NAMES,
            "schemas": BQ_TABLE_SCHEMAS,
        },
    )

    # End tasks to set trigger rules
    end_products = EmptyOperator(
        task_id='end_products',
        trigger_rule='all_done',
    )

    end_packs = EmptyOperator(
        task_id='end_packs',
        trigger_rule='all_done',
    )

    end_logs = EmptyOperator(
        task_id='end_logs',
        trigger_rule='all_done',
    )

    delay_task >> create_dataset >> create_table

    # Iterate over each competitor to create a loading task for each table
    for competitor in COMPETITORS:
        load_products = PythonOperator(
            task_id=f'load_products_{competitor}',
            python_callable=load_products_to_bq,
            op_kwargs={
                "client": client,
                "project_id": PROJECT_ID,
                "dataset_id": DATASET_ID,
                "competitor": competitor
            },
        )

        create_table >> load_products >> end_products

    for competitor in COMPETITORS:
        load_packs = PythonOperator(
            task_id=f'load_packs_{competitor}',
            python_callable=load_packs_to_bq,
            op_kwargs={
                "client": client,
                "project_id": PROJECT_ID,
                "dataset_id": DATASET_ID,
                "competitor": competitor
            },
        )

        end_products >> load_packs >> end_packs

    for competitor in COMPETITORS:
        load_logs = PythonOperator(
            task_id=f'load_logs_{competitor}',
            python_callable=load_logs_to_bq,
            op_kwargs={
                "client": client,
                "project_id": PROJECT_ID,
                "dataset_id": DATASET_ID,
                "competitor": competitor
            },
        )

        end_packs >> load_logs >> end_logs


load_job = load_to_bigquery_dag()
