from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from viking_scraper import mobile_viking_scraper



default_args = {
    'owner': 'admin',
    'retry': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(

    default_args=default_args,
    dag_id='scraping_dag',
    start_date=datetime(2023, 10, 5),
    schedule_interval=None,
    catchup=False,


) as dag:

    scrape_data = PythonOperator(
        task_id='scrape_data',
        python_callable=mobile_viking_scraper,
    )

    scrape_data
