import sys
sys.path.append('/opt/airflow')
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import configs.constants as Constants
from modules.collect_stocks import get_top_100_stocks, collect_stocks
from modules.refine_stocks import dataPreProcessing

stocks_etl_xcom_dag = DAG(
    dag_id="stocks_etl_xcom_dag",
    start_date=datetime(2024, 8, 10),
    schedule="@daily",
    description="ETL DAG to collect the NASDAQ stock data.",
    default_args={
        'owner': 'airflow',
        'depends_on_past': True,
        'email': ['sauravkumarbehera@gmail.com', 'shaswatranjan.odisha@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        # 'retries': 1,
        # 'retry_delay': timedelta(minutes=1),
        # 'start_date': days_ago(2),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'dag': dag,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    params=None,
    is_paused_upon_creation=False,
)

def initial_setup_task(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='data_lake_path', value=Constants.BASE_DATA_LAKE_PATH)

initial_setup = PythonOperator(
    task_id="initial_setup",
    dag=stocks_etl_xcom_dag,
    python_callable=initial_setup_task,
)

fetch_stocks_nasdaq = PythonOperator(
    task_id="fetch_stocks_nasdaq",
    dag=stocks_etl_xcom_dag,
    python_callable=get_top_100_stocks,
)

collect_stock_data = PythonOperator(
    task_id="collect_stock_data",
    dag=stocks_etl_xcom_dag,
    python_callable=collect_stocks,
)

convert_to_parquet = PythonOperator(
    task_id="convert_to_parquet",
    dag=stocks_etl_xcom_dag,
    python_callable=dataPreProcessing,
)

initial_setup >> fetch_stocks_nasdaq >> collect_stock_data >> convert_to_parquet