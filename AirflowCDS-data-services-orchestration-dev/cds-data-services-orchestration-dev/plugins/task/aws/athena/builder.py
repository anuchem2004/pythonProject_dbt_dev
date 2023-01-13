import re
import time
import boto3
from functools import partial
from plugins.task.common import BaseTask
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator

athena = boto3.client('athena')
s3 = boto3.client('s3')


class AthenaTableRepairTask(BaseTask):
    def __init__(self, args, dag=None):
        super(AthenaTableRepairTask, self).__init__(args)
        self.dag = dag

        self.task_name = args['type']
        self.database = args['database']
        self.table = args['table']
        self.query_location = args['query_location']
        self.fact_tables = args['fact_tables'] if 'fact_tables' in args else None
        self.retries = args['retries'] if 'retries' in args else 0

    def athena_msck_repair(self):

        repair_tables = ['%s.%s' % (self.database, self.table)]

        if self.fact_tables is not None:
            for fact_table in self.fact_tables:
                repair_tables.append('%s.%s' % (self.athena_source_db_fact, fact_table))

        for table in repair_tables:
            print('*** %s' % table)
            response = athena.start_query_execution(
                QueryString='MSCK REPAIR TABLE %s' % table,
                ResultConfiguration={
                    'OutputLocation': self.query_location,
                }
            )

            wait = True
            while wait:
                state = athena.get_query_execution(QueryExecutionId=response["QueryExecutionId"])
                status = state['QueryExecution']['Status']['State']
                print('%s - %s: %s' % (table, response["QueryExecutionId"], status))
                if status in ["QUEUED", "RUNNING"]:
                    time.sleep(5)
                elif status in ['FAILED', 'CANCELLED']:
                    wait = False
                    raise Exception('Repair table %s process status %s' % (table, status))
                elif status == 'SUCCEEDED':
                    wait = False

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        task_id = f'{index}_repair_table'
        return PythonOperator(
            task_id=task_id,
            python_callable=self.athena_msck_repair,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )


class AthenaTableDropTask(BaseTask):
    def __init__(self, args, dag=None):
        super(AthenaTableDropTask, self).__init__(args)
        self.dag = dag

        self.task_name = args['type']
        self.database = args['database']
        self.table = args['table']
        self.query_location = args['query_location']
        self.retries = args['retries'] if 'retries' in args else 0

    def generate(self, index, entity_task_id, is_final_task, prev_task):
        task_id = f'{index}_drop_athena_table'

        return AWSAthenaOperator(
            task_id=task_id,
            database=self.database,
            query='drop table if exists %s.%s' % (self.database, self.table),
            output_location=self.query_location,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )


class AthenaTableCreateTask(BaseTask):
    def __init__(self, args, dag=None):
        super(AthenaTableCreateTask, self).__init__(args)
        self.dag = dag

        self.task_name = args['type']
        self.destination = args['fully_qualified_destination']
        self.format = args['format']
        self.delimeter = args['delimeter']
        self.fully_qualified_object = args['fully_qualified_object']
        self.bucketed_by = args.get('bucketed_by', False)
        self.database = args['fully_qualified_destination'].split('.')[0]
        self.retries = args['retries'] if 'retries' in args else 0

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

        task_id = f"{index}_create_athena_table"
        return AWSAthenaOperator(
            task_id=task_id,
            database=self.database,
            query=create_query,
            output_location=self.query_location,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )

class Bookmark(BaseTask):
    def __init__(self, args, dag=None):
        super(Bookmark, self).__init__(args)
        self.dag = dag
        self.task_name = args['type']
        self.destination = args['fully_qualified_destination']
        self.source = args['fully_qualified_source']
        self.database = self.destination.split('.')[0]
        self.query_location = f"{re.sub('/$','',args['object_location'])}/query"
        self.export_location = f"{re.sub('/$','',args['object_location'])}/export"

    def execute_athena_query(self, query):
        query_start = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': self.database
            },
            ResultConfiguration={'OutputLocation': self.query_location}
        )

        wait = True
        while wait:
            state = athena.get_query_execution(QueryExecutionId=query_start["QueryExecutionId"])
            status = state['QueryExecution']['Status']['State']
            if status in ["QUEUED", "RUNNING"]:
                time.sleep(10)
            elif status in ['FAILED', 'CANCELLED']:
                wait = False
                raise Exception()
            elif status == 'SUCCEEDED':
                wait = False

    def create_bookmark_table(self):

        # Delete Bookmark table
        query = f'DROP TABLE IF EXISTS {self.destination}'
        self.execute_athena_query(query)

        # Clean athena export location
        bucket_name = self.export_location.replace('s3://', '').split('/', 1)[0]
        key = self.export_location.replace('s3://', '').split('/', 1)[1]

        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=key)
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                print('****Deleting = ', key)
                s3.delete_object(Bucket=bucket_name, Key=key)
            else:
                print('****No objects returned')

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

        task_id = f'{index}_bookmark'
        return PythonOperator(
            task_id=task_id,
            python_callable=self.create_bookmark_table,
            provide_context=True,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            dag=self.dag
        )
