from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.sensors.python import PythonSensor


from transform import clean_data_task
from utils import check_file_exist


HEADERS = ['products', 'packs', 'logs']
COMPETITORS = ['mobileviking', 'scarlet']
FILE_NAMES = ['products', 'packs', 'logs']

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
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

    # delay_task = TimeDeltaSensor(
    #     task_id='delay_task',
    #     delta=timedelta(seconds=170)
    # )

    wait_for_file = PythonSensor(
        task_id='wait_for_file',
        python_callable=check_file_exist,
        op_kwargs={"dir": "raw_data", "competitors": COMPETITORS, "file_names": FILE_NAMES, "file_type": "json"},
        mode='reschedule',
        timeout=200,

    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data_task,
        op_kwargs={'competitors': COMPETITORS, 'headers': HEADERS}
    )

    wait_for_file >> clean_data


clean_job = clean_dag()
