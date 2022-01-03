from typing import Tuple

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime


# from plugins.operators.flink_job_operator import FlinkJobOperator
from operators.flink_job_operator import FlinkJobOperator

# Constants
params = {
    's3_bucket': '0-atropos.mum1-pp.zetaapps.in',
    's3_key_prefix': 'flink/schedule_event_local_test/',
    'aws_conn_id': 'aws-preprod',
    'aws_region': 'ap-south-1',
    'flink_bin_path': '/Users/punit.s/Documents/GitHub/flink/flink-1.14.0/bin',
    'jar_path': '/Users/punit.s/Documents/GitHub/flink/flink-local-testing/target/flink-local-testing-1.0-SNAPSHOT.jar'
}


def parse_execution_time(ts_nodash) -> Tuple[str, str]:
    datetime_obj = datetime.strptime(ts_nodash, "%Y%m%dT%H%M%S")
    date = datetime_obj.date()
    time = datetime_obj.time()
    hh_mm = f'{time.hour:02}:{time.minute:02}'
    return str(date), hh_mm


def _generate_s3_paths(aws_conn_id,
                       aws_region,
                       s3_bucket,
                       s3_key_prefix,
                       task_instance,
                       ts_nodash):
    date, hh_mm = parse_execution_time(ts_nodash)
    s3_hook = S3Hook(aws_conn_id=aws_conn_id, region_name=aws_region)
    # Expected item in list: [flink/schedule_event_local_test/1/] (1 here is tenantId)
    prefix_list = s3_hook.list_prefixes(bucket_name=s3_bucket, prefix=s3_key_prefix, delimiter='/')

    s3_paths_list = []
    for prefix in prefix_list:
        # Note: prefix already contains forward slash('/') at the end
        key = f'{prefix}{date}/{hh_mm}/'
        key_exists = s3_hook.check_for_prefix(bucket_name=s3_bucket, prefix=key, delimiter='/')
        if key_exists:
            s3_path = f's3://{s3_bucket}/{key}'
            s3_paths_list.append(s3_path)

    task_instance.xcom_push(key='schedule_events_s3_paths', value=s3_paths_list)


def _check_s3_paths(task_instance) -> bool:
    s3_paths_list = task_instance.xcom_pull(
        key='schedule_events_s3_paths',
        task_ids=['generate_s3_paths']
    )[0]
    return len(s3_paths_list) > 0


with DAG(dag_id='schedule_events_dag',
         description='Atropos Schedule Events DAG',
         schedule_interval='* * * * *',
         tags=['atropos_schedule_events'],
         start_date=datetime(2021, 1, 1),
         params=params,
         catchup=False) as dag:
    generate_s3_paths = PythonOperator(
        task_id='generate_s3_paths',
        python_callable=_generate_s3_paths,
        op_kwargs={
            'aws_conn_id': '{{ params.aws_conn_id }}',
            'aws_region': '{{ params.aws_region }}',
            's3_bucket': '{{ params.s3_bucket }}',
            's3_key_prefix': '{{ params.s3_key_prefix }}'
        }
    )

    do_paths_exists = ShortCircuitOperator(
        task_id='are_paths_exists',
        python_callable=_check_s3_paths
    )

    submit_flink_job = FlinkJobOperator(
        task_id='submit_flink_job',
        flink_bin_path='{{ params.flink_bin_path }}',
        jar_path='{{ params.jar_path }}'
    )

    # Define dag
    generate_s3_paths >> do_paths_exists >> submit_flink_job
