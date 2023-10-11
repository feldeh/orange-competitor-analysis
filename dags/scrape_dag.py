from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator


from viking_scraper import mobileviking_scraper


DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}


@dag(
    dag_id='scrape_dag',
    start_date=datetime(2023, 10, 5),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS
)
def scrape_dag():
    scrape_data = PythonOperator(
        task_id='scrape_data',
        python_callable=mobileviking_scraper,
    )

    scrape_data


scrape_job = scrape_dag()
