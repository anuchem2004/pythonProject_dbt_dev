import re
from functools import partial
from plugins.v2.task.common import BaseTask
from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class AthenaTableRepairTask(BaseTask):
    def __init__(self, args, dag):
        super(AthenaTableRepairTask, self).__init__(args, dag)

        self.task_name = args.get('task_name', None)
        self.database = args['database']
        self.table = args['table']
        self.query_location = args['query_location']
        self.fact_tables = args.get('fact_tables', None)
        self.athena_source_db_fact = args.get('fact_table_database', None)
        self.retries = args.get('retries', 0)
        self.athena = None

    def athena_msck_repair(self):
        self.athena = AWSAthenaHook('Athena')

        repair_tables = [f'{self.database}.{self.table}']

        if self.fact_tables is not None:
            for fact_table in self.fact_tables:
                repair_tables.append(f'{self.athena_source_db_fact}.{fact_table}')

        for table in repair_tables:
            response = self.athena.run_query(
                query=f'MSCK REPAIR TABLE {table}',
                query_context={'Database': self.database},
                result_configuration={'OutputLocation': self.query_location}
            )
            status = self.athena.poll_query_status(query_execution_id=response)

            if status in self.athena.FAILURE_STATES:
                raise Exception(f'Repair table {table} process status {status}') 

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        self.set_task_name(index, 'repair_table')
        return PythonOperator(
            task_id=self.task_name,
            python_callable=self.athena_msck_repair,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )


class AthenaTableDropTask(BaseTask):
    def __init__(self, args, dag):
        super(AthenaTableDropTask, self).__init__(args, dag)

        self.task_name = args.get('task_name', None)
        self.database = args['database']
        self.table = args['table']
        self.query_location = args['query_location']
        self.retries = args.get('retries', 0)

    def generate(self, index, entity_task_id, is_final_task, prev_task):
        self.set_task_name(index, 'drop_athena_table')

        return AWSAthenaOperator(
            task_id=self.task_name,
            database=self.database,
            query=f'drop table if exists {self.database}.{self.table}',
            output_location=self.query_location,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )


class AthenaTableCreateTask(BaseTask):
    def __init__(self, args, dag):
        super(AthenaTableCreateTask, self).__init__(args, dag)

        self.task_name = args.get('task_name', None)
        self.destination = args['fully_qualified_destination']
        self.format = args['format']
        self.delimeter = args['delimeter']
        self.fully_qualified_object = args['fully_qualified_object']
        self.bucketed_by = args.get('bucketed_by', False)
        self.retries = args.get('retries', 0)
        self.database = args['fully_qualified_destination'].split('.')[0]

        object_location = re.sub('/$','',args.get('object_location',''))
        self.export_location = args['export_location'] if 'export_location' in args else f'{object_location}/export'
        self.query_location = args['query_location'] if 'query_location' in args else f'{object_location}/query'


    def generate(self, index, entity_task_id, is_final_task, prev_task):
        bucketed_by_parameters = ''
        if self.bucketed_by:
            bucketed_by_parameters = f", bucketed_by = ARRAY['{self.bucketed_by}'], bucket_count = 1"

        create_query = f'''CREATE TABLE {self.destination} WITH
        (
            format = '{self.format}',
            external_location = '{self.export_location}',
            field_delimiter = '{self.delimeter}'
            {bucketed_by_parameters}
        ) AS
        select * from {self.fully_qualified_object}
        '''

        self.set_task_name(index, 'create_athena_table')
        return AWSAthenaOperator(
            task_id=self.task_name,
            database=self.database,
            query=create_query,
            output_location=self.query_location,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )


class Bookmark(BaseTask):
    def __init__(self, args, dag):
        super(Bookmark, self).__init__(args, dag)
        self.destination = args['fully_qualified_destination']
        self.source = args['fully_qualified_source']
        self.task_name = args.get('task_name', None)
        self.retries = args.get('retries', 0)
        self.database = self.destination.split('.')[0]
        self.query_location = f"{re.sub('/$', '', args['object_location'])}/query"
        self.export_location = f"{re.sub('/$', '', args['object_location'])}/export"
        self.athena = None
        self.s3 = None

    def execute_athena_query(self, query):

        self.athena = AWSAthenaHook('Athena')
        response = self.athena.run_query(
            query=f'{query}',
            query_context={'Database': self.database},
            result_configuration={'OutputLocation': self.query_location}
        )
        status = self.athena.poll_query_status(query_execution_id=response)

        if status in self.athena.FAILURE_STATES:
            raise Exception(f'Bookmark process status {status}') 

    def create_bookmark_table(self):

        self.s3 = S3Hook("S3_default")
        # Delete Bookmark table
        query = f'DROP TABLE IF EXISTS {self.destination}'
        self.execute_athena_query(query)

        # Clean athena export location
        bucket_name = self.export_location.replace('s3://', '').split('/', 1)[0]
        location = self.export_location.replace('s3://', '').split('/', 1)[1]

        keys = self.fetch_keys(bucket_name, location)

        if keys is not None:
            self.s3.delete_objects(bucket=bucket_name, keys=keys)
        else:
            print('****No objects returned, No files to delete')

        # Create Bookmark table in athena
        query = f'''CREATE TABLE {self.destination} WITH
        (
            format = 'TEXTFILE',
            external_location = '{self.export_location}',
            field_delimiter = '|'
        ) AS
        select * from {self.source} '''

        self.execute_athena_query(query)

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        self.set_task_name(index, 'bookmark')
        return PythonOperator(
            task_id=self.task_name,
            python_callable=self.create_bookmark_table,
            provide_context=True,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )
