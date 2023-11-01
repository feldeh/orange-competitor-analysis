from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from viking_scraper import mobileviking_scraper
from scarlet_scraper import scarlet_scraper
from utils import read_config_from_json


DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}


@dag(
    dag_id='scrape_dag',
    start_date=datetime(2023, 10, 5),
    schedule_interval=None,
    description="Scraping data",
    catchup=False,
    default_args=DEFAULT_ARGS
)
def scrape_dag():

    config = read_config_from_json()

    scrape_viking = PythonOperator(
        task_id='scrape_viking',
        python_callable=mobileviking_scraper,
        op_args=[config['mobileviking']]
    )

    scrape_scarlet = PythonOperator(
        task_id='scrape_scarlet',
        python_callable=scarlet_scraper,
    )

    # Allow DAG to continue execution regardless of upstream task outcomes
    end_task = EmptyOperator(
        task_id='end_task',
        trigger_rule='all_done',
    )

    [scrape_viking, scrape_scarlet] >> end_task


scrape_job = scrape_dag()
