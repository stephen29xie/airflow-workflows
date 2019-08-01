"""
Example use of EmailOperator
"""

from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from datetime import datetime

default_args = {
    'owner': 'Stephen Xie',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 20),
    'email': ['stephenxie.airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    dag_id='example_email_operator',
    default_args=default_args,
    schedule_interval='@once',
    max_active_runs=1,
    catchup=False
)

test_email = EmailOperator(task_id='test_email',
                           to='stephenxie.airflow@gmail.com',
                           subject='Test EmailOperator',
                           html_content='<h1>Hello from Airflow</h1>',
                           dag=dag)

test_email
