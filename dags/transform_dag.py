from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator


from transform import clean_data_task

HEADERS = ['products', 'packs']

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}


@dag(
    dag_id='clean_dag',
    start_date=datetime(2023, 10, 5),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS
)
def clean_dag():
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data_task,
        op_kwargs={'headers': HEADERS}
    )

    clean_data


clean_job = clean_dag()
