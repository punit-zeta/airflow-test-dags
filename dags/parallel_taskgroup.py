from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG('parallel_taskgroup', schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_taskgroup') as processing_taskgroup:
        task_2 = BashOperator(
            task_id='task_2',
            bash_command='sleep 3'
        )

        with TaskGroup('spark_task') as spark_task:
            task_3 = BashOperator(
                task_id='task_3',
                bash_command='sleep 3'
            )

        with TaskGroup('flink_task') as flink_task:
            task_3 = BashOperator(
                task_id='task_3',
                bash_command='sleep 3'
            )

    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 3'
    )

    # Parallel DAG
    task_1 >> processing_taskgroup >> task_4
