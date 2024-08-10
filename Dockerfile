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
RUN pip install yfinance
RUN pip install pyarrow

COPY ./dags /opt/airflow/dags
COPY ./modules /opt/airflow/modules
COPY ./scripts /opt/airflow/scripts
COPY ./utils /opt/airflow/utils
COPY ./configs /opt/airflow/configs
