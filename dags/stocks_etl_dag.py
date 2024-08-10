from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import configs.constants as Constants

stocks_etl_dag = DAG(
    dag_id="stocks_etl_dag",
    start_date=datetime(2024, 8, 1),
    schedule="@daily",
    description="ETL DAG to collect the NASDAQ stock data.",
    default_args={
        'owner': 'airflow',
        'depends_on_past': True,
        'email': ['sauravkumarbehera@gmail.com', 'shaswatranjan.odisha@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=3),
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

collect_books_task = BashOperator(
    task_id="collect_stocks",
    dag=stocks_etl_dag,
    env={{"DATA_LAKE_PATH": Constants.BASE_DATA_LAKE_PATH}},
    bash_command=f"python3 {Constants.BASE_MODULE_PATH}collect_stocks.py --data_lake_path $DATA_LAKE_PATH",
)

refine_books_task = BashOperator(
    task_id="refine_stocks",
    dag=stocks_etl_dag,
    env={"DATA_LAKE_PATH": Constants.BASE_DATA_LAKE_PATH},
    bash_command=f"python3 {Constants.BASE_MODULE_PATH}refine_stocks.py --data_lake_path $DATA_LAKE_PATH",
)

collect_books_task >> refine_books_task