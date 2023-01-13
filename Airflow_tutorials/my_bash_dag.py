from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG('my_bash_dag', start_date=datetime(2022,1,1),schedule_interval='@daily',catchup=False) as dag:
    execute_command = BashOperator(
    task_id="execute_command",
    bash_command= "echo 'execute my command'"
)

execute_command