FROM apache/airflow:2.7.1
ADD requirements.txt .
RUN pip install apache-airflow==2.7.1 -r requirements.txt