#!/bin/sh

echo "Starting airflow DB init and user Create!"

airflow db init
airflow users create \
  --username "airflow" \
  --firstname "airflow" \
  --lastname "airflow" \
  --role Admin \
  --email "airflow@gmail.com" \
  --password "admin"

airflow standalone