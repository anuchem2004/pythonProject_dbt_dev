from functools import partial
from plugins.v2.task.common import BaseTask
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException


class S3CopyFileTask(BaseTask):
    """
    Copy Files from source s3 to destination s3 with matching pattern 
    Returns list of copied files
    """

    def __init__(self, args, dag):
        super(S3CopyFileTask, self).__init__(args, dag)
        self.s3 = None
        self.source_bucket = args['source_bucket']
        self.source_location = args['source_location']
        self.destination_bucket = args['target_bucket']
        self.destination_location = args['target_location']
        self.source_pattern = args.get('source_pattern', '')
        self.retries = args.get('retries', 0)
        self.task_name = args.get('task_name', None)

    def copy(self):
        self.s3 = S3Hook("S3_default").get_conn()
        destination_keys = []
        source_keys = self.fetch_keys(self.source_bucket, self.source_location, self.source_pattern)

        for s_key in source_keys:
            file_name = s_key.split("/")[-1]
            d_key = f'{self.replace_placeholder(self.destination_location)}/{file_name}'
            destination_keys.append(d_key)

            copy_source = {
                'Bucket': self.source_bucket,
                'Key': s_key
            }

            self.s3.copy(
                CopySource=copy_source,
                Bucket=self.destination_bucket,
                Key=d_key
            )

        if not destination_keys:
            raise AirflowSkipException('Task Error: No file found to copy from specified source location.')

        return destination_keys

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        self.set_task_name(index, 'copy_S3_file')
        return PythonOperator(
            task_id=self.task_name,
            python_callable=self.copy,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )


class S3ChangeMetadataTask(BaseTask):
    """
    Change s3 file's metadata to content_encoding from configuration
    Returns list of changed files
    """

    def __init__(self, args, dag):
        super(S3ChangeMetadataTask, self).__init__(args, dag)
        self.bucket = args['bucket_name']
        self.location = args['location']
        self.pattern = args.get('pattern', '')
        self.encoding = args.get('content_encoding', 'gzip')
        self.retries = args.get('retries', 0)
        self.s3 = None
        self.task_name = args.get('task_name', None)

    def change_s3_metadata(self):

        self.s3 = S3Hook("S3_default").get_conn()
        keys = self.fetch_keys(self.bucket, self.location, self.pattern)

        for key in keys:
            copy_source = {
                'Bucket': self.bucket,
                'Key': key
            }

            self.s3.copy(
                CopySource=copy_source,
                Bucket=self.bucket,
                Key=key,
                ExtraArgs={'ContentEncoding': self.encoding, 'MetadataDirective': 'REPLACE'}
            )
        if not keys:
            raise AirflowSkipException('Task Error: No files to change metadata')

        return keys

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        self.set_task_name(index, 'change_metadata')
        return PythonOperator(
            task_id=self.task_name,
            python_callable=self.change_s3_metadata,
            provide_context=True,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )


class S3DeleteFileTask(BaseTask):
    """
    Delete files from s3 location with matching pattern, if pattern is not in args delete all files
    """

    def __init__(self, args, dag):
        super(S3DeleteFileTask, self).__init__(args, dag)
        self.bucket = args['bucket_name']
        self.location = args['location']
        self.pattern = args.get('pattern', '')
        self.retries = args.get('retries', 0)
        self.task_name = args.get('task_name', None)
        self.s3 = None

    def delete_s3_file(self):
        self.s3 = S3Hook("S3_default")

        keys = self.fetch_keys(self.bucket, self.location, self.pattern)

        if keys is not None:
            print('****Deleting = ', keys)
            self.s3.delete_objects(bucket=self.bucket, keys=keys)
        else:
            print('****No objects returned, No files to delete')

    def generate(self, index, entity_task_id, is_final_task, prev_task):

        self.set_task_name(index, 'delete_S3_file')
        return PythonOperator(
            task_id=self.task_name,
            python_callable=self.delete_s3_file,
            provide_context=True,
            on_success_callback=partial(self.on_success_callback, entity_task_id, is_final_task),
            on_failure_callback=partial(self.on_failure_callback, entity_task_id),
            retries=self.retries,
            dag=self.dag
        )
