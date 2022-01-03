from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator

# from plugins.operators.flink_job_operator import FlinkJobOperator
# from operators.flink_job_operator import FlinkJobOperator
from datetime import datetime, timedelta

params = {
    's3_bucket': '0-atropos.mum1-pp.zetaapps.in',
    'aws_conn_id': 'aws-preprod',
    'aws_region': 'ap-south-1',
    'flink_bin_path': '/Users/punit.s/Documents/GitHub/flink/flink-1.14.0/bin',
    'jar_path': '/Users/punit.s/Documents/GitHub/flink/flink-local-testing/target/flink-local-testing-1.0-SNAPSHOT.jar',
    'job_args': ''
}


def _change_params(**context):
    context['a'] = 'b'
    print(context['params'])
    print(type(context))
    print(context['params']['s3_bucket'])


def _check_params(**context):
    print(context['a'])
    print(context['params'])
    print(context['params']['job_args'])


with DAG(dag_id='schedule_events_test_dag',
         description='Atropos Schedule Events Test DAG',
         schedule_interval='* * * * *',
         tags=['atropos_schedule_events'],
         start_date=datetime(2021, 1, 1),
         params=params,
         catchup=False) as dag:
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=_change_params
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=_check_params
    )

    # submit_flink_job = BashOperator(
    #     task_id='submit_flink_job',
    #     bash_command='sleep 30',
    #     execution_timeout=timedelta(seconds=10)
    # )

    # submit_flink_job = FlinkJobOperator(
    #     task_id='submit_flink_job',
    #     flink_bin_path='{{ params.flink_bin_path }}',
    #     jar_path='{{ params.jar_path }}',
    #     job_args=generate_flink_job_args(),
    #     execution_timeout=timedelta(minutes=10)
    # )

    task_1 >> task_2