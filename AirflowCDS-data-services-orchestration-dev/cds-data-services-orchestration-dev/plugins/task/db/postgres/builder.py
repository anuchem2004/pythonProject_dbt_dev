from functools import partial
from plugins.task.common import BaseTask
from airflow.providers.postgres.operators.postgres import PostgresOperator


class PostgresSprocExecutionTask(BaseTask):
    def __init__(self, args, dag=None):
        self.dag = dag
        self.task_name = args['type']
        self.connection_id = args['connection_id']
        self.sproc_name = args['sproc_name']
        self.params = args['params'] if 'params' in args else None
        self.retries = args['retries'] if 'retries' in args else 0

    def build_sql(self,prev_task_id):
        query = None
        param_values = []
        
        if self.params is not None:
            for key in sorted(self.params.keys()):
                if '{s3_key}' in self.params[key]:
                    param_values.append("{{ task_instance.xcom_pull(task_ids='%s') }}" % prev_task_id)
                else:
                    param_values.append(self.params[key])
        
        # 'cds-core-qa','channel'

        sproc_params = ''
        if param_values:
            sproc_params = "'%s'" % "','".join(param_values)

        query = f'CALL {self.sproc_name}({sproc_params})'
        
        # print(f'PostgresSprocExecutionTask: {query}')
        return query

    def generate(self, index, entity_task_id, is_final_task, prev_task_id):
        task_id = f'{index}_load_data' 

        query = self.build_sql(prev_task_id)
        
        return PostgresOperator(
            task_id=task_id,
            postgres_conn_id=self.connection_id,
            sql=query,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )
