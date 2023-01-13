from functools import partial
from plugins.v2.task.common import BaseTask
from airflow.providers.postgres.operators.postgres import PostgresOperator


class PostgresQueryExecutionTask(BaseTask):
    def __init__(self, args, dag=None):
        self.dag = dag
        self.connection_id = args['connection_id']
        self.query = args['query']
        self.retries = args.get('retries', 0)
        self.task_name = args.get('task_name', None)

    def build_sql(self,prev_task_id):
        sql = self.query
        if '{s3_key}' in sql:
            sql = sql.replace('{s3_key}', f"{{{{ task_instance.xcom_pull(task_ids='{prev_task_id}') }}}}")
        
        return sql

    def generate(self, index, entity_task_id, is_final_task, prev_task_id):
        
        self.set_task_name(index, 'load_data')
        sql = self.build_sql(prev_task_id)
        
        return PostgresOperator(
            task_id=self.task_name,
            postgres_conn_id=self.connection_id,
            sql=sql,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )
