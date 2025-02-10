import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import TaskInstance
from src.collection_algorithm import listen_new_data, test1, send_live_game_data

kst = pendulum.timezone('Asia/Seoul')
dag_name = "nba_data_pipeline"

default_args = {
    'owner': 'Euizzang',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.datetime(2025, 2, 7, 12, 0, 0, tz=kst),
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
    def check_listen_new_data(**kwargs):
        task_instance = kwargs['task_instance']
        should_continue = listen_new_data(task_instance)
        return "continue_task" if should_continue else "stop_task"


    branch_task = BranchPythonOperator(
        task_id="listen_new_data",
        python_callable=check_listen_new_data,
        provide_context=True,
    )

    continue_task = EmptyOperator(task_id="continue_task")
    stop_task = EmptyOperator(task_id="stop_task")

    send_live_game_tasks = []
    for i in range(15):
        task = PythonOperator(
            task_id=f"live_game{i}_task",
            python_callable=send_live_game_data,
            op_kwargs={"game_num": i},
        )
        send_live_game_tasks.append(task)

    branch_task >> continue_task >> send_live_game_tasks
    branch_task >> stop_task








