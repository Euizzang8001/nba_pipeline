import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.collection_algorithm import listen_new_data

kst = pendulum.timezone('Asia/Seoul')
dag_name = "nba_data_pipeline"

default_args = {
    'owner': 'Euizzang',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': kst,
    'depends_on_past': False,
    'email': ['qkrdmlcks55@naver.com'],
    'email_on_failure': False,
    'email_on_retry': False
}
with DAG(
    dag_id = dag_name,
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
    description = 'DAG for nba_data_pipeline',
    tags = ['nba', 'euizzang', 'euichan', 'data', 'data engineering']
) as dag:
    listen_new_data_task = PythonOperator(
        task_id="listen_new_data_task",
        python_callable=listen_new_data,
    )

    listen_new_data_task