# Airflow DAG Custom Plugins
## Version: `2.0`

## Plugins:
- **db**: Package for database communication
    - data_ops.py: ORM classes of `data_ops` schema
        - `TestCases`
        - `TestCaseLog`

    - oms_ods_admin.py: ORM classes of `oms_ods_admin` schema
        - `CdsDataProcessingStatusConfiguration`
        - `CdsDataProcessingStatus`
- **task**: Package to build DAG tasks dynamically
    - **s3**
        - builder.py: Task builder classes for `AWS S3 service` 
            - `S3CopyFileTask`
            - `S3ChangeMetadataTask`
            - `S3DeleteFileTask`
    
    - **Athena**
        - builder.py: Task builder classes for `AWS Athena service`
            - `AthenaTableRepairTask`
            - `AthenaTableDropTask`
            - `AthenaTableCreateTask`
            - `Bookmark`

    - **EMR**
        - builder.py: Task builder classes for `EMR Job Execution`
            - `EmrTaskExecutor`

    - **Postgres**
        - builder.py: Task builder classes for `Postgres Service` 
            - `PostgresQueryExecutionTask`
    
    - common.py: Shared functionalities for all task builder classes
        - `BaseTask`
        - `DummyTask`
        - `EntityTask`
        - `DiscoverFileTask`
        - `ParallelGroupEndTask`

    - builder.py: Classes for dynamic task creation
        - `TaskFactory`
        - `TaskBuilder`
        - `CdsDataProcessor`

## Steps to create dag on the basis of task configuration
1. Insert data source details in  `oms_ods_admin.airflow_plugin_dag_configuration`
2. Insert task configuration details in `oms_ods_admin.airflow_plugin_task_configuration`

_Note: To build the task configuration use the configuration sample from git wiki (https://github.com/CDSGlobal/cds-data-services-orchestration/wiki)  or from the airflow_plugin_task_type config_details column
3. Create dag with DATA_SOURCE (example as follows):

```python

from airflow import DAG 
from plugins.task.builder import TaskBuilder 

DAG_NAME = 'dag_test_example' 
DATA_SOURCE = 'dag_test_example' 
 
with DAG( 
        dag_id=DAG_NAME, 
        schedule_interval='@daily', 
        catchup=False, 
        max_active_runs=1, 
        concurrency=2
) as dag: 
    task_builder = TaskBuilder(dag) 
    task_builder.generate_task_pipeline(DATA_SOURCE) 

```

## Use of custom plugins
Airflow custom plugins is a way to customize our own Airflow installation to reflect organizations ecosystem contain common utility code to share among multiple DAGs.

## How to use plugins
Copy all files and folders present in `plugins` to AWS S3 under `[airflow bucket]/dags/plugins/` location of Airflow setup directory

While creating a DAG, all custom plugins will be available under plugin package

## Example:
Following code snippet show how to import `CdsDataProcessingStatusConfiguration` ORM class present in `db.oms_ods_admin` of custom plugin.

```python
from plugins.db.oms_ods_admin import CdsDataProcessingStatusConfiguration

CdsDataProcessingStatusConfiguration().discover_tables('dag_test_example')
```

## Reference: [Installing custom plugins](https://docs.aws.amazon.com/mwaa/latest/userguide/configuring-dag-import-plugins.html)