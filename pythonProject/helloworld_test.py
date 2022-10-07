from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Following are defaults which can be overridden later on
def helloWorld():
    print("Hello World")

def helloWorld1():
    print("Hello task2")
with DAG(dag_id="HelloWorld_dag",
        start_date=datetime(2022, 10, 3),
        # 03 Oct 2022
        schedule= "@hourly",
        catchup=False) as dag:

# t1, t2, t3 and t4 are examples of tasks created using operators

        task1 = PythonOperator(
            task_id='hello_world',
            python_callable=helloWorld1())


task1
