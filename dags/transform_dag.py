from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.sensors.python import PythonSensor


from transform import clean_data_task


HEADERS = ['products', 'packs', 'logs']
COMPETITORS = ['mobileviking', 'scarlet']

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}


@dag(
    dag_id='clean_dag',
    start_date=datetime(2023, 10, 5),
    schedule_interval=None,
    description="Transforming data",
    catchup=False,
    default_args=DEFAULT_ARGS
)
def clean_dag():

    delay_task = TimeDeltaSensor(
        task_id='delay_task',
        delta=timedelta(seconds=2)
    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data_task,
        op_kwargs={'competitors': COMPETITORS, 'headers': HEADERS}
    )

    delay_task >> clean_data


clean_job = clean_dag()
