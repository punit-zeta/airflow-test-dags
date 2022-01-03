from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.utils.log.logging_mixin import LoggingMixin

flink_base_url = 'http://localhost:8081'


class FlinkSubmitHook(SubprocessHook, LoggingMixin):

    def get_conn(self) -> Any:
        pass

    def start_cluster(self, flink_bin_path):
        if self.__flink_cluster_running():
            self.log.info("Flink cluster is already running")
        else:
            self.__start_flink_cluster(flink_bin_path)
            self.log.info("Flink cluster started")
        pass

    def submit_job(self, flink_bin_path: str, jar_path: str, job_args: dict):
        flink_submit_jar = f'{flink_bin_path}/flink run {jar_path}'
        job_args_str = ''
        for key in job_args.keys():
            job_args_str = f'{job_args_str} {key} {job_args[key]}'

        self.log.info(f'Job args str: {job_args_str}')
        self.__execute_bash_command(f'{flink_submit_jar + job_args_str}', "Submitting flink job...")

    def __flink_cluster_running(self) -> bool:
        try:
            url = f'{flink_base_url}/overview'
            response = requests.get(url)
            return 200 <= response.status_code <= 299
        except requests.ConnectionError:
            return False

    def __start_flink_cluster(self, flink_bin_path):
        start_cluster_script = f'{flink_bin_path}/start-cluster.sh'
        self.__execute_bash_command(start_cluster_script, f"Starting flink cluster at {flink_base_url}")

    def __execute_bash_command(self, bash_command, msg):
        self.log.info(msg)
        result = self.run_command(
            command=['bash', '-c', bash_command]
        )
        if result.exit_code != 0:
            raise AirflowException(
                f'Bash command failed. The command returned a non-zero exit code {result.exit_code}.'
            )
        pass
