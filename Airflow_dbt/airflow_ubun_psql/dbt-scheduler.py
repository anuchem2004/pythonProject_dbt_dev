from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from distutils.log import debug
from faker import Faker
import psycopg2
from random import randint, randrange
from datetime import datetime
from faker.providers import DynamicProvider
import random
import os
from urllib import response
import json
from werkzeug.utils import secure_filename
import sys
from airflow.models import Variable


fake = Faker('en-US')



def connection(connection_name):
    f = open('/usr/local/airflow/dags/config.json')
    config = json.load(f)
    if connection_name=='postgres':
        return config['postgres_host_name'],config['postgres_db_name'],config['postgres_port'],config['postgres_user'],config['postgres_passwd']
    else :
        return config['redshift_host_name'],config['redshift_db_name'],config['redshift_port'],config['redshift_user'],config['redshift_passwd']

indexes = 1
seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                    datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '* 1 * * *',

    }



dag = DAG('dbt_scheduler', default_args=default_args, catchup=False)


dbt_run = BashOperator(
task_id='dbt_run_scheduler',
bash_command="dbt run  --full-refresh --profiles-dir  /tmp/dbt-orchestration/ --project-dir /tmp/dbt-orchestration/dbt_redshift_poc -m curate curate1",
dag=dag)


dbt_run
