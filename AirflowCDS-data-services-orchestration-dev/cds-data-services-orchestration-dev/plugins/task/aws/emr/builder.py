import json
from functools import partial
from datetime import timedelta
from airflow.models import taskinstance
from plugins.task.common import BaseTask
from airflow.utils.db import provide_session
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator

class DataExtractor(BaseTask):

    def __init__(self, args, dag=None):
        super(DataExtractor, self).__init__(args)
        self.dag = dag
        self.task_name = args['type']
        self.cluster_id = args['cluster_id']
        self.emr_task_name = args['emr_task_name']
        self.emr_job_name = args['job_name']
        self.job_type = args.get('job_type', None)
        self.app_path = args['app_path']
        self.discovered_file_key = args.get('loader_xcom_key', None)
        self.step_args = args['step_args']
        self.retries = args.get('retries', 0)
          

    def get_steps(self, index):
        
        values = []
        
        if self.discovered_file_key:
            val1 = "{{ task_instance.xcom_pull(key='%s')[0] }}" % self.discovered_file_key
            val2 = "{{ task_instance.xcom_pull(key='%s')[1] }}" % self.discovered_file_key
            
            values.append(val1)
            values.append(val2)
        else:
            prev_task_id = f"{index.split('_')[0]}_discover_{self.job_type}"

            values.append("{{ task_instance.xcom_pull(task_ids='%s', key='return_value')[0] }}"% prev_task_id)
            values.append("{{ task_instance.xcom_pull(task_ids='%s', key='return_value')[1] }}"% prev_task_id)

        updated_config=self.step_args['config'].replace('BEFOREIMAGE',"'BEFOREIMAGE'")
        config = json.loads(self.replace_placeholder(json.dumps(updated_config), values))

        emr_steps = {
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
                            "processor=%s" % self.step_args['processor'],
                            "config=%s" % config
                        ]
                    }
                }

        return emr_steps

    def generate(self, index, entity_task_id, is_final_task, prev_task):
        task_id = f'{index}_{self.emr_task_name}_step'
        em_steps=self.get_steps(index)
        return EmrAddStepsOperator(
            task_id=task_id,
            job_flow_id=self.cluster_id,
            steps=(json.loads(json.dumps(em_steps)),),
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            do_xcom_push=True,
            dag=self.dag,
            execution_timeout=timedelta(minutes=10),
            weight_rule='upstream',
            retries=self.retries
        )
  

class EMRJobStepSensor(BaseTask):

    def __init__(self, args, dag=None):
        self.dag = dag
        self.cluster_id = args['cluster_id']
        self.task_name = args['task_name']    
        self.retries = args.get('retries', 0)   


    @provide_session
    def clear_task(self, task_ins, session = None, adr = False, dag = None):
        taskinstance.clear_task_instances(
            tis = task_ins,
            session = session,
            activate_dag_runs = adr,
            dag = dag)

    def retry_upstream_task(self,context):
        tasks = context["dag_run"].get_task_instances()
        tasks_to_retry = []
        upstream_tasks = context['task'].upstream_task_ids      
        for task in tasks:            
            if task.task_id in upstream_tasks:
                tasks_to_retry.append(task)
                      
        print(f"***Previous TI: {tasks_to_retry}")
        self.clear_task(tasks_to_retry, dag = self.dag)


    def generate(self, index, entity_task_id, is_final_task, prev_task):
        task_id = f'{index}_{self.task_name}_sensor'
      
        return EmrStepSensor(
            task_id=task_id,
            job_flow_id=self.cluster_id,
            step_id="{{ task_instance.xcom_pull(task_ids='%s', key='return_value')[0] }}"% (prev_task),
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),                
            dag=self.dag,
            execution_timeout=timedelta(minutes=120),
            retry_delay=timedelta(minutes=5),
            on_retry_callback=self.retry_upstream_task,
            weight_rule='upstream',
            retries=self.retries
        )