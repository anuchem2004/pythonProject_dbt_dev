import re
import boto3
import socket
import requests
from datetime import date
from functools import partial
from plugins.db.oms_ods_admin import CdsDataProcessingStatus
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State

SLACK_CHANNEL_TOKEN = {
    "damic_prod_events": "T4333K3AQ/B02529EUTDF/MdDdLROlt9pzDrrg3eyIzfJQ",
    "damic_nonprod_events": "T4333K3AQ/B02529EUTDF/5qaPYOgzpQByeMFX73QROeX2",
    "damic_slack_notification_testing": "T4333K3AQ/B02529VFY1F/OEM9d3N2hXMSGsCG3Tbbf9PJ"
}

class SlackNotification:

    def __init__(self, args={}):
        self.host = 'hooks.slack.com'
        self.env = 'QA'
        # self.uri = 'services/%s' % SLACK_CHANNEL_TOKEN[args['slack_channel']] if 'slack_channel' in args else \
        # 'services/%s' % SLACK_CHANNEL_TOKEN['damic_slack_notification_testing']

        if 'slack_channel' in args:
            self.uri = 'services/%s' % SLACK_CHANNEL_TOKEN[args['slack_channel']]
        else:
            self.uri = 'services/%s' % SLACK_CHANNEL_TOKEN['damic_slack_notification_testing']

        self.URL = 'https://%s/%s' % (self.host, self.uri)

    def __send_request(self, message):
        try:
            payload = {"text": message}
            x = requests.post(self.URL, json=payload)

        except Exception as e:
            print('Notify Error: %s' % e)
            raise e

    def __build_message(self, raw_message):

        dag_name = raw_message['dag_name']
        task_name = raw_message['task_name']
        error_msg = raw_message['message']
        processing_id = raw_message['processing_id']

        message = '''<!here|here>\nEnvironment : `%s`\nDag Name : `%s`\nTask Name : `%s`\nProcessing ID : `%s`\nError Details :```%s``` \
        ''' % (self.env, dag_name, task_name, processing_id, error_msg)
        return message

    def notify(self, message):
        formatted_message = self.__build_message(message)
        self.__send_request(formatted_message)


class BaseTask(object):
    """docstring for BaseTask"""

    is_final_task = None

    def __init__(self, args):
        super(BaseTask, self).__init__()
        self.arg = args
        self.is_final_task = False
        # self.sn = SlackNotification(args)

    def on_success_callback(self, task_name, is_final_task, context):
        if is_final_task:
            ti = context['ti']
            processing_id = ti.xcom_pull(task_ids=task_name)
            CdsDataProcessingStatus().mark_as_completed(processing_id)

    def on_failure_callback(self, task_name, context):
        exception = context.get('exception')
        hostname = socket.gethostname()
        formated_exception = str(hostname) + str(exception).replace("'", " ")
        ti = context['ti']
        processing_id = ti.xcom_pull(task_ids=task_name)

        CdsDataProcessingStatus().mark_as_failed(processing_id, formated_exception)

    def mark_cdc_processing_status_as_skip(self, processing_id):
        msg = 'No file found to process'
        CdsDataProcessingStatus().mark_as_skipped(processing_id, msg)

    def replace_placeholder(self, value, values=None):
        placeholders = re.findall(r'{\w+?}', value)

        for raw_cmd in placeholders:
            cmd = raw_cmd.replace('{', '').replace('}', '')

            if cmd == 'current_date':
                value = value.replace(raw_cmd, date.today().strftime("%Y-%m-%d"))

            if cmd == 'xcom_pull_1':
                value = value.replace(raw_cmd, values[0])

            if cmd == 'xcom_pull_2':
                value = value.replace(raw_cmd, values[1])

        return value

    def fetch_key(self, bucket_name, prefix, source_pattern):
        s3 = boto3.client('s3')

        my_key = None
        result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' in result:
            for content in result['Contents']:
                if content['Size'] > 0 and source_pattern in content['Key']:
                    my_key = content['Key']
                    break
        else:
            print('****Invalid s3 location')

        print('****result=%s' % result)
        print('****my_key= %s' % my_key)
        return my_key


class DummyTask:
    def __init__(self, args, dag=None):
        self.task_name = args['name']
        self.trigger_rule = args['trigger_rule'] if 'trigger_rule' in args else 'all_done'
        self.dag = dag

    def generate(self, index=None):
        if index is not None:
            self.task_name

        return DummyOperator(
            task_id=self.task_name,
            dag=self.dag,
            trigger_rule=self.trigger_rule
        )


class EntityTask(BaseTask):
    def __init__(self, args, dag=None):
        super(EntityTask, self).__init__(args)
        self.table_name = args['name']
        self.config_id = args['config_id']
        self.dag = dag
        self.retries = args.get('retries', 0)
        self.track_status = args.get('track_status', True)

    def insert_processing_status(self, **kwargs):
        task_insts = kwargs['ti'].get_dagrun().get_task_instances()

        parent_task_ids = list(kwargs['task'].upstream_task_ids)
        is_parent_success = False

        for task_inst in task_insts:
            if task_inst.task_id in parent_task_ids and task_inst.state == State.SUCCESS:
                is_parent_success = True
                break

        if is_parent_success:
            if self.track_status:
                query = '''
                    INSERT INTO oms_ods_admin.cds_data_processing_status(
                        config_id,                
                        status)
                    SELECT 
                        config_id,                
                        'Discovered'
                    From oms_ods_admin.cds_data_processing_status_configuration where config_id = %s returning processing_id 
                ''' % self.config_id

                result = CdsDataProcessingStatus().custom_insert(query)
                return result[0]
            else:
                return 0
        else:
            print("All parent tasks either failed or skipped")
            raise AirflowSkipException

    def generate(self, index):
        task_id = f'{index}_{self.table_name}'
        return PythonOperator(
            task_id=task_id,
            python_callable=self.insert_processing_status,
            on_failure_callback=partial(self.on_failure_callback, task_id),
            dag=self.dag,
            trigger_rule='all_done',
            retries=self.retries
        ), task_id


class DiscoverFile(BaseTask):
    def __init__(self, args, dag=None):
        super(DiscoverFile, self).__init__(args)
        self.task_name = args['type']
        self.config_id = args['config_id']
        self.file_type = args['file_type']
        self.pattern = args['pattern']
        self.entity_type = args['entity_type']
        self.dag = dag
        self.retries = args['retries'] if 'retries' in args else 0
        self.bucket = args['bucket']
        self.key = args['key']
        self.s3 = boto3.client('s3')

    def has_unprocessed_files(self):
        
        query = f'''
            SELECT 1
            FROM oms_ods_admin.cds_data_processing_file 
            WHERE config_id={self.config_id}
            AND name LIKE '%{self.pattern}%'
            AND processing_id is null
        '''
        res = CdsDataProcessingStatus().custom_select(query)
        return res

    def get_discovered_file_list(self):
        query = f"SELECT out_file_list, out_processing_date FROM oms_ods_admin.f_get_discovered_files({self.config_id}, '{self.pattern}')"
        return CdsDataProcessingStatus().custom_select(query)[0]

    def set_processsing_id(self, processing_id, files):
        query = f"SELECT updated_count FROM oms_ods_admin.f_set_processing_id({self.config_id}, {processing_id}, '{files}')"
        CdsDataProcessingStatus().custom_select(query)        

    def process_entry(self, **kwargs):
        # Get the list of files to be processed for the given config_id.
        key = f"{self.key}/{self.entity_type}.csv"

        result = None
        if self.has_unprocessed_files():
            processing_id = str(kwargs['processing_id'])
            
            # Get list of files and processing_date.
            result = self.get_discovered_file_list()
            files_str = result[0]
            s3_location = None

            try:
                if 'FULL-LOAD' in files_str:
                    files = list(filter(lambda x: 'FULL-LOAD' in x, files_str.split(',')))[0]
                    s3_location = files
                else:
                    files = files_str
                    self.s3.put_object(Bucket=self.bucket, Key=key, Body=files_str.replace(',','\n'))
                    s3_location = f's3://{self.bucket}/{key}'

                self.set_processsing_id(processing_id, files)
            
            except Exception as e:
                raise e

        else:
            raise AirflowSkipException('No files to process. Hence not creating a record in status table.')



        return s3_location, result[1]

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        task_id = f"{index.split('_')[0]}_discover_{self.entity_type}"
        processing_id = "{{ task_instance.xcom_pull(task_ids='%s') }}" % entity_task_id
        return PythonOperator(
            task_id=task_id,
            python_callable=self.process_entry,
            op_kwargs={'processing_id': processing_id},
            on_success_callback=partial(self.on_success_callback, task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, task_id),
            dag=self.dag,
            weight_rule='upstream',
            retries=self.retries
        )


class ParallelGroupEnd(BaseTask):
    def __init__(self, args, dag=None):
        super(ParallelGroupEnd, self).__init__(args)
        self.task_id = args['task_id']
        self.op_kwargs = args['op_kwargs']
        self.trigger_rule = args.get('trigger_rule', 'all_done')
        self.dag = dag
        self.retries = args.get('retries', 0)

    def get_done_tasks(self, **kwargs):
        task_insts = kwargs['ti'].get_dagrun().get_task_instances()
        succ_task_id = None
        loader_key = kwargs['xcom_loader_key']

        parent_task_ids = list(kwargs['task'].upstream_task_ids)
        is_parent_success = False
        is_parent_failed = False
        disc_identifier = '_discover_%s' % loader_key
        for task_inst in task_insts:
            if task_inst.task_id in parent_task_ids and task_inst.state == State.SUCCESS:
                is_parent_success = True
            if task_inst.state == State.SUCCESS and disc_identifier in task_inst.task_id:
                succ_task_id = task_inst.task_id
            if task_inst.task_id in parent_task_ids and (
                    task_inst.state == State.FAILED or task_inst.state == State.UPSTREAM_FAILED):
                is_parent_failed = True

        if is_parent_success:
            if succ_task_id:
                val = kwargs['ti'].xcom_pull(task_ids=succ_task_id, key='return_value')
                print(type(val))
                print(val)
                if loader_key:
                    kwargs['ti'].xcom_push(key=loader_key, value=val)
            else:
                print("No discovered task found.")
        else:
            print("Both Tasks either failed or skipped")
            if not is_parent_failed:
                entity_task = kwargs['entity-task']
                processing_id = kwargs['ti'].xcom_pull(task_ids=entity_task, key='return_value')
                self.mark_cdc_processing_status_as_skip(processing_id)

            raise AirflowSkipException

    def generate(self):
        return PythonOperator(task_id=self.task_id,
                              dag=self.dag,
                              python_callable=self.get_done_tasks,
                              op_kwargs=self.op_kwargs,
                              trigger_rule=self.trigger_rule,
                              provide_context=True,
                              retries=self.retries)