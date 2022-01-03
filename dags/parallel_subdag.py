from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.parallel_subdag import subdag_of_task23

from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG('parallel_subdag', schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 3'
    )

    processing_subdag = SubDagOperator(
        task_id='processing_subdag',
        subdag=subdag_of_task23('parallel_subdag', 'processing_subdag', default_args)
    )

    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 3'
    )

    # Parallel DAG
    task_1 >> processing_subdag >> task_4
