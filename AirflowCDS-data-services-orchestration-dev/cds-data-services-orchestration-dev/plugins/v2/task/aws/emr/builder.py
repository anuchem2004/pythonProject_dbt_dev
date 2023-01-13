import json
import time
from functools import partial
from datetime import timedelta
from plugins.v2.task.common import BaseTask
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook


class EmrTaskExecutor(BaseTask):
    """EmrTaskExecutor: EMR job executor executes and wait for job completion of the EMR job."""

    def __init__(self, args, dag):
        """ Do Not initialize aws connection in init """
        super(EmrTaskExecutor, self).__init__(args, dag)
        self.app_path = args['app_path']
        self.cluster_id = args['cluster_id']
        self.emr_job_name = args['job_name']
        self.step_args = args['step_args']
        self.job_type = args.get('job_type', None)
        self.discovered_file_key = args.get('loader_xcom_key', None)
        self.retries = args.get('retries', 0)
        self.task_name = args.get('task_name', None)
        self.poll_interval_seconds = 30
        self.index = None
        self.emr_hook = None
        self.running_states = {'PENDING', 'RUNNING'}
        self.failed_states = {'FAILED', 'CANCELLED', 'INTERRUPTED', 'CANCEL_PENDING'}

    def get_job_config(self, **kwargs):

        xcom_values = None
        if self.discovered_file_key:
            xcom_values = kwargs['ti'].xcom_pull(key=self.discovered_file_key)
        else:
            prev_task_id = f"{self.index.split('_')[0]}_discover_{self.job_type}"
            xcom_values = kwargs['ti'].xcom_pull(task_ids=prev_task_id, key='return_value')

        updated_config = self.step_args['config'].replace('BEFOREIMAGE', "'BEFOREIMAGE'")

        config = None
        if xcom_values:
            config = json.loads(self.replace_placeholder(json.dumps(updated_config), xcom_values))

        emr_step = {
            "Name": self.emr_job_name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    self.step_args['deploy-mode'],
                    "--master",
                    "yarn",
                    "--class",
                    self.step_args['class'],
                    "--name",
                    self.step_args['--name'],
                    self.app_path,
                    f"processor={self.step_args['processor']}",
                    f"config={config}"
                ]
            }
        }
        return emr_step

    def get_job_status(self, step_id):
        self.emr_hook = EmrHook(aws_conn_id='aws_default').get_conn()
        response = self.emr_hook.describe_step(ClusterId=self.cluster_id, StepId=step_id)
        return response["Step"]["Status"]["State"]

    def poll_query_status(self, step_id):
        running = True
        while running:
            time.sleep(self.poll_interval_seconds)
            try:
                status = self.get_job_status(step_id)

            except Exception as e:
                raise Exception(f'Task got error as {e}. for more information check log')

            if status in self.running_states:
                running = True
            elif status in self.failed_states:
                raise Exception(f'EMR Step failed {status}')
            else:
                running = False

    def run_job(self, **kwargs):

        self.emr_hook = EmrHook(aws_conn_id='aws_default').get_conn()
        # Get configuration object
        step = self.get_job_config(**kwargs)

        # Submit job to EMR
        response = self.emr_hook.add_job_flow_steps(JobFlowId=self.cluster_id, Steps=[step])

        # Poll for the job completion status
        step_id = response['StepIds'][0]
        self.poll_query_status(step_id)

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        self.set_task_name(index, 'emr_job')
        self.index = index

        return PythonOperator(
            task_id=self.task_name,
            python_callable=self.run_job,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            do_xcom_push=True,
            provide_context=True,
            dag=self.dag,
            weight_rule='upstream',
            execution_timeout=timedelta(minutes=130),
            retries=self.retries
        )
