FROM apache/airflow:2.7.1
ADD requirements.txt .
RUN pip install apache-airflow==2.7.1 -r requirements.txt && \
    playwright install
USER root
RUN apt update && apt-get install -y \
       libglib2.0-0 \
       libnss3 \
       libnspr4 \
       libatk1.0-0 \
       libatk-bridge2.0-0 \
       libcups2 \
       libdbus-1-3 \
       libdrm2 \
       libxcb1 \
       libxkbcommon0 \
       libatspi2.0-0 \
       libx11-6 \
       libxcomposite1 \
       libxdamage1 \
       libxext6 \
       libxfixes3 \
       libxrandr2 \
       libgbm1 \
       libpango-1.0-0 \
       libcairo2 \
       libasound2

