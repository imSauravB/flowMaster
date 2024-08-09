FROM apache/airflow:slim-2.9.3-python3.11
ENV AIRFLOW_HOME=/opt/airflow
USER root
RUN apt update
RUN apt install -y \
    gcc \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    pkg-config
USER airflow
RUN pip install mysqlclient

COPY entrypoint.sh /opt/airflow/
