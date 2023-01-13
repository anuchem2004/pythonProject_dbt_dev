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

def input_customers(connection_name):
    print(connection(connection_name))
    hostname, database, port_id, username, pwd = connection(connection_name)
    conn = psycopg2.connect(host=hostname, database=database,user=username, password=pwd, port=port_id)
    cur = conn.cursor()
    cur.execute('''create table if not exists postgres.airflow_plugin_task_type (task_type_id bigserial NOT NULL,
        task_type_name text NOT NULL,
        description text NULL,
        config_details json NULL,
        created_dt timestamp NULL DEFAULT now(),
        CONSTRAINT airflow_plugin_task_type_pkey1 PRIMARY KEY (task_type_id)''')

    cur.execute("ROLLBACK")

    cur.execute("select count(*) from postgres.advance_jaffle_shop.customers")
    length1 = cur.fetchone()
    print(length1)
    index1 = 1 if length1[0] < 1 else length1[0] + 1
    customer_data = {}
    customer_data['cid'] = index1
    name1 = fake.name()
    customer_data['f_name'] = name1.split()[0]
    customer_data['l_name'] = name1.split()[1]
    try:
        cur.execute("insert into postgres.advance_jaffle_shop.customers values('{}','{}','{}')".format(customer_data['cid'],customer_data['f_name'],customer_data['l_name']))
        conn.commit()
        ds = {
            'Customer id': customer_data['cid'],
            'First Name': customer_data['f_name'],
            'Last Name': customer_data['l_name'],
        }
        ds = str(ds)
        print(ds)
        return json.dumps({'ds': ds})

    except:
        print("Waiting.....")


dag = DAG('generator', start_date=datetime(2022,10,18), catchup=False)

generate_customers = PythonOperator(
    task_id='generate_customers',
    python_callable=input_customers,
    op_kwargs={'connection_name': 'postgres'},

    dag=dag
)

def main():

    input_customers("postgres")

if __name__ == '__main__':
    try:
        while True:
            globals()[sys.argv[1]](sys.argv[2])
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)



generate_customers

