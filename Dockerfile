FROM apache/airflow:3.2.1

USER root

# Install system dependencies needed to compile psycopg2 and dbt
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        libpq-dev \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

# Install additional Python packages into the Airflow image
RUN pip install --no-cache-dir -r /requirements.txt
