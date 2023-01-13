import time
import boto3
from functools import partial
from plugins.task.common import BaseTask
from airflow.operators.python import PythonOperator

S3 = boto3.client('s3')

class S3CopyFileTask(BaseTask):

    def __init__(self, args, dag=None):
        super(S3CopyFileTask, self).__init__(args)
        self.dag = dag

        self.task_name = args['type']
        self.source_bucket = args['source_bucket']
        self.source_location = args['source_location']
        self.destination_bucket = args['target_bucket']
        self.destination_location = args['target_location']
        self.source_pattern = args['source_pattern']
        self.retries = args['retries'] if 'retries' in args else 0

    def copy(self):
        source_key = self.fetch_key(self.source_bucket, self.replace_placeholder(self.source_location),
                                    self.source_pattern)
        destination_key = None

        if source_key is not None:
            file_name = source_key.split("/")[-1]
            destination_key = '%s/%s' % (self.replace_placeholder(self.destination_location), file_name)
            copy_source = {
                'Bucket': self.source_bucket,
                'Key': source_key
            }
            S3.copy(copy_source, self.destination_bucket, destination_key)
        else:
            raise Exception('Task Error: No file found to copy from specified source location.')

        return destination_key

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        task_id = f'{index}_copy_S3_file'
        return PythonOperator(
            task_id=task_id,
            python_callable=self.copy,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )

class S3ChangeMetadataTask(BaseTask):

    def __init__(self, args, dag=None):
        super(S3ChangeMetadataTask, self).__init__(args)
        self.dag = dag
        self.task_name = args['type']
        self.bucket_name = args['s3_bucket']
        self.location = args['location']
        self.pattern = args['pattern']
        self.retries = args['retries'] if 'retries' in args else 0
        # self.change_all = args.get('change_all', False)

    def change_s3_metadata(self):

        key = self.fetch_key(self.bucket_name, self.replace_placeholder(self.location), self.pattern)

        if key is not None:

            copy_source = {
                'Bucket': self.bucket_name,
                'Key': key
            }
            S3.copy(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=key,
                ExtraArgs={'ContentEncoding': 'gzip', 'MetadataDirective': 'REPLACE'}
            )

        else:
            raise Exception('Task Error: No files to change metadata')

        return key

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        task_id = f'{index}_change_metadata'
        return PythonOperator(
            task_id=task_id,
            python_callable=self.change_s3_metadata,
            provide_context=True,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )

class S3DeleteFileTask(BaseTask):

    def __init__(self, args, dag=None):
        super(S3DeleteFileTask, self).__init__(args)
        self.dag = dag
        self.task_name = args['type']
        self.bucket_name = args['s3_bucket']
        self.location = args['location']
        self.pattern = args['pattern']
        self.delete_all_files = args['delete_all']
        self.retries = args['retries'] if 'retries' in args else 0

    def delete_s3_file(self):

        if self.delete_all_files:
            key = self.location
        else:
            key = self.fetch_key(self.bucket_name, self.replace_placeholder(self.location), self.pattern)

        if key is not None:
            response = S3.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
            if 'Contents' in response:
                for object in response['Contents']:
                    key = object['Key']
                    print('****Deleting = ', key)
                    S3.delete_object(Bucket=self.bucket_name, Key=key)
                    for i in range(3):
                        if self.fetch_key(self.bucket_name, self.replace_placeholder(self.location),
                                          self.pattern) == key:
                            S3.delete_object(Bucket=self.bucket_name, Key=key)
                        else:
                            break
                        time.sleep(3)
            else:
                print('****No objects returned')
        else:
            print('****No files to delete')

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        task_id = f'{index}_delete_S3_file'
        return PythonOperator(
            task_id=task_id,
            python_callable=self.delete_s3_file,
            provide_context=True,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )
