from datetime import timedelta
from airflow import DAG
from airflow.decorators import dag
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta

with DAG(
        dag_id='dbt_demo',
        start_date=datetime(2021, 12, 23),
        description='An Airflow DAG to invoke simple dbt commands',
        schedule_interval=timedelta(days=1),
) as dag:
    dbt_debug = SSHOperator(
        task_id='dbt_debug',
        ssh_conn_id='ssh_new',
        command='cd /home/ec2-user/redshift_demo; dbt debug '
    )

    wt_email_individual_bridge_distinct_alterian = SSHOperator(
        task_id='wt_email_individual_bridge_distinct_alterian',
        ssh_conn_id='ssh_new',
        command='cd /home/ec2-user/redshift_demo; dbt run --full-refresh -m wt_email_individual_bridge_distinct_alterian'
    )

    wt_individual_alterian_driver = SSHOperator(
        task_id='wt_individual_alterian_driver',
        ssh_conn_id='ssh_new',
        command='cd /home/ec2-user/redshift_demo; dbt run --full-refresh -m wt_individual_alterian_driver'
    )

    wt_individual_f = SSHOperator(
        task_id='wt_individual_f',
        ssh_conn_id='ssh_new',
        command='cd /home/ec2-user/redshift_demo; dbt run --full-refresh -m wt_individual_f'
    )

    update_source_last_send_individual = SSHOperator(
        task_id='update_source_last_send_individual',
        ssh_conn_id='ssh_new',
        command='cd /home/ec2-user/redshift_demo; dbt run --full-refresh -m update_source_last_send_individual'
    )

    dbt_test = SSHOperator(
        task_id='dbt_test',
        ssh_conn_id='ssh_new',
        command='cd /home/ec2-user/redshift_demo; dbt test'
    )

    dbt_debug >> wt_email_individual_bridge_distinct_alterian >> wt_individual_alterian_driver >> wt_individual_f >> update_source_last_send_individual >> dbt_test