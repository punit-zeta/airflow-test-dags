from functools import cached_property
from typing import Optional, Dict

from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator

from hooks.flink_submit_hook import FlinkSubmitHook


class FlinkJobOperator(BaseOperator):
    template_fields = ('flink_bin_path', 'jar_path')

    def __init__(self,
                 flink_bin_path: str,
                 jar_path: str,
                 job_args: Optional[Dict] = None,
                 **kwargs) -> None:
        """
        Custom Flink Job operator to submit flink job locally.
        :param flink_bin_path: The path of the bin folder in flink distribution folder
        :param jar_path: The path of jar present on the dev local machine
        :param job_args: Optional flink job args
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.flink_bin_path = flink_bin_path
        self.jar_path = jar_path
        self.job_args = job_args or dict()

    @cached_property
    def flink_submit_hook(self):
        """ Returns hook to submit flink jobs """
        return FlinkSubmitHook()

    def execute(self, context: Dict):
        """
        1. Start the flink cluster if it is not running
        2. Generate job args
        3. Submit flink job
        """
        self.flink_submit_hook.start_cluster(self.flink_bin_path)

        ti: TaskInstance = context['task_instance']
        self.generate_job_args(ti)

        self.flink_submit_hook.submit_job(self.flink_bin_path, self.jar_path, self.job_args)

    def generate_job_args(self, ti):
        s3_paths_list = ti.xcom_pull(key='schedule_events_s3_paths',
                                     task_ids=['generate_s3_paths'])[0]
        total_paths = len(s3_paths_list)
        self.job_args['--totalPaths'] = total_paths

        for path_index in range(0, total_paths):
            key = f'--s3Path{path_index}'
            value = s3_paths_list[path_index]
            self.job_args[key] = value
        pass
