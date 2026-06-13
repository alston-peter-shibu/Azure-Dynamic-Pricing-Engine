from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'pricing_pipeline',
    start_date=datetime(2024,1,1),
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/dbt/pricing_project && dbt run --profiles-dir /opt/dbt'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/dbt/pricing_project && dbt test --profiles-dir /opt/dbt'
    )

    dbt_run >> dbt_test
