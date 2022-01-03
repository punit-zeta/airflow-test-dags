from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from random import uniform

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def _training_model(task_instance):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    task_instance.xcom_push(key="model_accuracy", value=accuracy)


def _choose_best_model(task_instance):
    print('choose best model')
    accuracies = task_instance.xcom_pull(key="model_accuracy", task_ids=[
        'processing_tasks.training_model_a',
        'processing_tasks.training_model_b',
        'processing_tasks.training_model_c'
    ])

    for accuracy in accuracies:
        if accuracy > 5:
            return 'task_accurate'

    return 'task_inaccurate'


with DAG('path_specific_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        do_xcom_push=False
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model
        )

    choose_model = BranchPythonOperator(
        task_id='choose_model',
        python_callable=_choose_best_model
    )

    task_accurate = DummyOperator(
        task_id='task_accurate'
    )

    task_inaccurate = DummyOperator(
        task_id='task_inaccurate'
    )

    # Storing will now execute by specifing correct trigger rule
    # Note: Here we can use none_failed rule also. But it doesn't give guarantee that at least 
    # 1 parent task succeed. By using none_failed_or_skipped will give guarantee that atleast 1 parent
    # task succeed. Hence it will be best suited for our case.
    storing = DummyOperator(
        task_id='storing',
        trigger_rule='none_failed_or_skipped'
    )

    downloading_data >> processing_tasks >> choose_model >> [task_accurate, task_inaccurate] >> storing
