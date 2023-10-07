from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {
    'owner': 'admin',
    'retry': 5,
    'retry_delay': timedelta(minutes=1)
}


@dag(
    dag_id='master_dag',
    start_date=datetime(2023, 9, 21),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS
)
def master_dag():

    trigger_scraping = TriggerDagRunOperator(
        task_id='trigger_scraping',
        trigger_dag_id='scraping_dag',
    )

    # trigger_cleaning = TriggerDagRunOperator(
    #     task_id='trigger_cleaning',
    #     trigger_dag_id='cleaning_dag',
    # )

    trigger_load_to_bigquery = TriggerDagRunOperator(
        task_id='trigger_load_to_bigquery',
        trigger_dag_id='load_to_bigquery_dag',
    )

    trigger_scraping >> trigger_load_to_bigquery


# instantiate the dag
etl_dag = master_dag()
